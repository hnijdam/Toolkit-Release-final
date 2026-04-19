"use strict";
/*
 *   Copyright (c) 2025 I.C.Y. B.V.
 *   Author: MerijnHNE
 *   All rights reserved.
 *   This file and code may not be modified, reused, or distributed without the prior written consent of the author or organisation.
 */

const path = require('path');
const dotenv = require('dotenv');

const ENV_CANDIDATES = [
    path.join(__dirname, '.env'),
    path.join(__dirname, '..', 'python', 'DBscript', '.env'),
    path.join(__dirname, '..', '..', 'python', 'DBscript', '.env'),
    path.join(process.cwd(), '.env')
];

for (const envPath of ENV_CANDIDATES) {
    try {
        dotenv.config({ path: envPath, override: false });
    } catch (e) {
        // ignore and continue
    }
}

if (!process.env.DB_URL1 && process.env.DB_HOST) process.env.DB_URL1 = process.env.DB_HOST;
if (!process.env.DB_URL2 && process.env.DB_HOST2) process.env.DB_URL2 = process.env.DB_HOST2;
if (!process.env.DB_USERNAME && process.env.DB_USER) process.env.DB_USERNAME = process.env.DB_USER;
if (!process.env.DB_URL_PORT && process.env.DB_PORT) process.env.DB_URL_PORT = process.env.DB_PORT;
if (!process.env.DB_URL_PORT) process.env.DB_URL_PORT = '3306';

const mysql = require('mysql2/promise');
const fs = require('fs');
const readline = require('readline');
const XLSX = require("xlsx-js-style");
const chalk = require('chalk');
// Change the inquirer pointer from the default '>' to a green arrow
try {
    const figures = require('figures');
    // Use a nice green arrow glyph for the pointer
    try {
        figures.pointer = chalk.green('➜');
    } catch (e) {
        figures.pointer = '➜';
    }
    // also set a smaller pointer variant if available
    try {
        figures.pointerSmall = chalk.green('›');
    } catch (e) {
        figures.pointerSmall = '›';
    }
    // Map chalk.cyan / cyanBright to chalk.green so the pointer is green
    if (chalk) {
        if (chalk.green) {
            chalk.cyan = chalk.green;
            if (chalk.cyanBright) chalk.cyanBright = chalk.green;
        } else if (chalk.keyword) {
            // fallback
            chalk.cyan = chalk.keyword('green');
            chalk.cyanBright = chalk.keyword('green');
        }
    }
} catch (e) {
    // ignore if figures isn't available for some reason
}

// Export stats: schemas that have modules with schakeltijd < 60s
// For each schema: schema name, count of modules with schakeltijd < 60, distinct swversions for those modules
// Also returns total number of customers (schemas) with at least one such module
async function export_schakeltijden_under60_stats(openAfter = true) {
    await fillSchemas();
    const pool = getPool();
    const connection = await pool.getConnection();
    const rowsOut = [];
    const allRevs = new Set();
    try {
        for (const schema of allSchemas) {
        if (['information_schema', 'mysql', 'performance_schema', 'sys'].includes(schema)) continue;
        try {
            const [rows] = await connection.query(`SELECT curconfig, swversion FROM ${schema}.slavedevice WHERE slavedevid = 8705`);
            let countUnder60 = 0;
            const revCounts = {};
            const rawSet = new Set();
            for (const r of rows) {
                try {
                    if (!r.curconfig || r.curconfig.length < 10) continue;
                    const hex = r.curconfig.slice(8, 10);
                    const sec = parseInt(hex, 16);
                    if (!Number.isNaN(sec) && sec < 60) {
                        countUnder60++;
                        // derive revision like 'sw21' from swversion '485021' using preferred pattern
                        let revLabel = 'sw_unknown';
                        if (r.swversion) {
                            const sv = String(r.swversion).trim();
                            rawSet.add(sv);
                            let rev = null;
                            const m = sv.match(/4850(\d{2})/);
                            if (m) {
                                rev = m[1];
                            } else {
                                const m2 = sv.match(/(\d{2})$/);
                                if (m2) rev = m2[1];
                            }
                            if (rev) revLabel = `sw${rev}`;
                        }
                        revCounts[revLabel] = (revCounts[revLabel] || 0) + 1;
                        allRevs.add(revLabel);
                    }
                } catch (e) { continue; }
            }
            if (countUnder60 > 0) {
                const rowObj = { Schema: schema, ModulesUnder60: countUnder60, RawSWVersions: Array.from(rawSet).join('; ') };
                // attach rev counts (filled later to ensure consistent columns)
                Object.assign(rowObj, revCounts);
                rowsOut.push(rowObj);
            }
        } catch (e) {
            if (e && e.code === 'ER_NO_SUCH_TABLE') continue;
            console.error(chalk.red(`Fout bij schema ${schema}: ${e.message || e}`));
        }
    }
    } finally {
        try { connection.release(); } catch (e) {}
    }

    const totalCustomers = rowsOut.length;

    const ws = XLSX.utils.json_to_sheet(rowsOut);
    // Ensure consistent columns: Schema, ModulesUnder60, then sorted revision columns
    const revCols = Array.from(allRevs).sort();
    // force 'sw_unknown' to the end if present
    const idx_unknown = revCols.indexOf('sw_unknown');
    if (idx_unknown !== -1) {
        revCols.splice(idx_unknown, 1);
        revCols.push('sw_unknown');
    }
    // ensure each row has all revision columns set (default 0)
    for (const r of rowsOut) {
        for (const c of revCols) {
            if (typeof r[c] === 'undefined' || r[c] === null) r[c] = 0;
        }
    }
    const headers = ['Schema', 'ModulesUnder60', ...revCols];

    // compute totals per revision and total modules
    const totals = { Schema: 'Totaal', ModulesUnder60: 0 };
    for (const c of revCols) totals[c] = 0;
    for (const r of rowsOut) {
        totals.ModulesUnder60 += (r.ModulesUnder60 || 0);
        for (const c of revCols) totals[c] += (r[c] || 0);
    }

    // append totals row
    rowsOut.push(totals);

    const wsOrdered = XLSX.utils.json_to_sheet(rowsOut, { header: headers });
    if (rowsOut.length > 0 && wsOrdered['!ref']) {
        const range = XLSX.utils.decode_range(wsOrdered['!ref']);
        for (let col = range.s.c; col <= range.e.c; col++) {
            const cellAddress = XLSX.utils.encode_cell({ r: 0, c: col });
            if (!wsOrdered[cellAddress]) continue;
            wsOrdered[cellAddress].s = { font: { bold: true, sz: 11 }, alignment: { vertical: 'center', horizontal: 'left' } };
        }
        wsOrdered['!autofilter'] = { ref: XLSX.utils.encode_range(range) };
        const colsWidth = [ { wch: 40 }, { wch: 12 } ].concat(revCols.map(() => ({ wch: 12 })));
        wsOrdered['!cols'] = colsWidth;
    }

    const wb = XLSX.utils.book_new();
    XLSX.utils.book_append_sheet(wb, wsOrdered, 'Schakeltijden<60');
    const filename = `icy4850_schakeltijden_under60_stats_${new Date().toISOString().slice(0,19).replace(/[:T]/g,'-')}.xlsx`;
    const fullPath = path.join(EXPORT_DIR, filename);
    XLSX.writeFile(wb, fullPath, { cellStyles: true });

    console.log(chalk.green(`STATISTIEK OPSLAAN GELUKT: ${fullPath}`));
    console.log(chalk.green(`Totaal klanten met minimaal 1 module <60s: ${totalCustomers}`));

    if (openAfter) {
        try {
            const openAnswer = await inquirer.prompt([{ type: 'confirm', name: 'open', message: 'Bestand openen?', default: true }]);
            if (openAnswer.open) await openFile(fullPath);
        } catch (e) { /* ignore */ }
    }

}
// Intercept stdout/stderr writes to color any angle-bracket tokens green
try {
    const _stdoutWrite = process.stdout.write.bind(process.stdout);
    process.stdout.write = (chunk, encoding, cb) => {
        try {
            const isBuffer = Buffer.isBuffer(chunk);
            let str = isBuffer ? chunk.toString(encoding || 'utf8') : String(chunk);
            // Match <token> even when wrapped in ANSI color sequences and replace with green
            str = str.replace(/(?:\u001b\[[0-9;]*m)*<([^>]+)>(?:\u001b\[[0-9;]*m)*/g, (m, p1) => chalk.green(`<${p1}>`));
            // Also remap ANSI cyan colors (normal and bright) to green so question marks and helpers appear green
            str = str.replace(/\u001b\[36m/g, '\u001b[32m').replace(/\u001b\[96m/g, '\u001b[92m').replace(/\x1b\[36m/g, '\x1b[32m').replace(/\x1b\[96m/g, '\x1b[92m');
            const out = isBuffer ? Buffer.from(str, encoding || 'utf8') : str;
            return _stdoutWrite(out, encoding, cb);
        } catch (e) {
            return _stdoutWrite(chunk, encoding, cb);
        }
    };

    const _stderrWrite = process.stderr.write.bind(process.stderr);
    process.stderr.write = (chunk, encoding, cb) => {
        try {
            const isBuffer = Buffer.isBuffer(chunk);
            let str = isBuffer ? chunk.toString(encoding || 'utf8') : String(chunk);
            str = str.replace(/(?:\u001b\[[0-9;]*m)*<([^>]+)>(?:\u001b\[[0-9;]*m)*/g, (m, p1) => chalk.green(`<${p1}>`));
            str = str.replace(/\u001b\[36m/g, '\u001b[32m').replace(/\u001b\[96m/g, '\u001b[92m').replace(/\x1b\[36m/g, '\x1b[32m').replace(/\x1b\[96m/g, '\x1b[92m');
            const out = isBuffer ? Buffer.from(str, encoding || 'utf8') : str;
            return _stderrWrite(out, encoding, cb);
        } catch (e) {
            return _stderrWrite(chunk, encoding, cb);
        }
    };
} catch (e) {
    // ignore
}
const inquirer = require('inquirer');
const { exec } = require('child_process');

// Monkeypatch inquirer.prompt to color angle-bracketed tokens green (e.g. <space>, <enter>)
// This ensures help accents shown by some prompts appear green.
try {
    const _origPrompt = inquirer.prompt.bind(inquirer);
    inquirer.prompt = async function (questions) {
        const colorize = (q) => {
            if (q && typeof q.message === 'string') {
                q.message = q.message.replace(/<([^>]+)>/g, (m, p1) => chalk.green(`<${p1}>`));
            }
            return q;
        };
        if (Array.isArray(questions)) {
            questions = questions.map(colorize);
        } else {
            questions = colorize(questions);
        }
        return _origPrompt(questions);
    };
} catch (e) {
    // ignore
}

// Inquirer UI settings
const INQUIRER_PAGE_SIZE = 12;

// Helper to render lettered menus (e.g. "A. Foo") for consistent look with toolkit.ps1
function buildLetteredChoices(items) {
    // items: array of { letter: 'A', name: 'Description', value: 'A' }
    return items.map(it => ({ name: `${it.letter}. ${it.name}`, value: it.value }));
}

function printMenuHeader(title) {
    const line = '-'.repeat(110);
    console.log(chalk.gray(line));
    console.log(chalk.bold.white(title));
}

function renderMainHeader(dbChoice) {
    console.clear();
    const startup = '************ STARTUP 4850CM DB TOOLS ************';
    console.log(chalk.cyan.bold(startup));
    if (dbChoice === 'A') console.log(chalk.green('Geselecteerde Database: MySQL'));
    else if (dbChoice === 'B') console.log(chalk.green('Geselecteerde Database: MariaDB'));
    else console.log(chalk.green('Geselecteerde Database: (niet geselecteerd)'));
    console.log(chalk.gray('Gebruik pijltjes om te navigeren, Enter om te selecteren.'));
    console.log('');
}

async function chooseSchemaInteractive(message = 'Welk database-schema (organisatie)?') {
    // Verbeterde NL-tekst: 'Welk database-schema' is grammaticaal correcter
    await fillSchemas();
    const choices = allSchemas.map(s => ({ name: s, value: s }));
    if (choices.length === 0) {
        throw new Error('Geen schema\'s beschikbaar om te kiezen.');
    }
    const answer = await inquirer.prompt([
        { type: 'list', name: 'schema', message: message, choices: choices, pageSize: INQUIRER_PAGE_SIZE }
    ]);
    return answer.schema;
}
const EXPORT_DIR = "C:\\Users\\h.nijdam\\Documents\\ICY-Logs";

// Ensure export directory exists
if (!fs.existsSync(EXPORT_DIR)){
    fs.mkdirSync(EXPORT_DIR, { recursive: true });
}

// Simple run logfile to trace actions (append-only)
const RUN_LOGFILE = EXPORT_DIR + '\\icy4850_run_' + new Date().toISOString().slice(0,19).replace(/[:T]/g,'-') + '.log';
function writeLog(msg, level = 'INFO') {
    const line = `[${new Date().toISOString()}] [${level}] ${msg}\n`;
    try {
        fs.appendFileSync(RUN_LOGFILE, line, { encoding: 'utf8' });
    } catch (e) {
        console.error('KAN LOG NIET SCHRIJVEN:', e.message || e);
    }
    try { console.log(chalk.gray('[LOG] ' + msg)); } catch (e) { console.log('[LOG] ' + msg); }
}

// helper to write CSV report rows (simple, safe quoting via JSON.stringify)
function writeCsvReport(filename, rows) {
    try {
        if (!rows || rows.length === 0) {
            fs.writeFileSync(filename, 'No rows.\n', 'utf8');
            return;
        }
        const headers = Object.keys(rows[0]);
        const lines = [headers.join(',')];
        for (const r of rows) {
            const cols = headers.map(h => {
                const v = typeof r[h] === 'undefined' || r[h] === null ? '' : String(r[h]);
                return JSON.stringify(v);
            });
            lines.push(cols.join(','));
        }
        fs.writeFileSync(filename, lines.join('\n'), 'utf8');
    } catch (e) {
        console.error('FOUT BIJ OPSLAAN CSV:', e.message || e);
    }
}

console.clear();

// Removed readline interface as we are switching to inquirer for main interaction
// Keeping it only if needed for specific legacy parts, but better to remove if fully refactoring.
// For now, I will comment it out to avoid conflicts.
/*
const rl = readline.createInterface({
    input: process.stdin,
    output: process.stdout
})
*/

let dbUrl;
let DB_URL_PORT = process.env.DB_URL_PORT;
let dbUsername = process.env.DB_USERNAME;
let dbPassword = process.env.DB_PASSWORD;

// Cached list of schemas
let allSchemas = [];

// Connection pool (created lazily when dbUrl is set)
let dbPool = null;
function getPool() {
    if (!dbUrl) throw new Error('Database URL (dbUrl) is not set. Kies eerst een database.');
    if (!dbPool) {
        const limit = parseInt(process.env.DB_POOL_LIMIT || '10', 10) || 10;
        dbPool = mysql.createPool({
            host: dbUrl,
            port: DB_URL_PORT,
            user: dbUsername,
            password: dbPassword,
            waitForConnections: true,
            connectionLimit: limit,
            queueLimit: 0,
            ssl: { rejectUnauthorized: false }
        });
        writeLog(`POOL CREATED for ${dbUrl} (limit=${limit})`);
    }
    return dbPool;
}

async function closePool() {
    if (!dbPool) return;
    try {
        await dbPool.end();
        writeLog(`POOL CLOSED for ${dbUrl || 'n/a'}`);
    } catch (e) {
        console.error(chalk.yellow(`Kon DB pool niet netjes sluiten: ${e.message || e}`));
    } finally {
        dbPool = null;
    }
}

async function fillSchemas(force = false) {
    if (!force && Array.isArray(allSchemas) && allSchemas.length > 0) return allSchemas;
    if (!dbUrl) throw new Error('Database URL (dbUrl) is not set. Kies eerst een database.');
    let connection = null;
    try {
        const pool = getPool();
        connection = await pool.getConnection();
        const [rows] = await connection.query('SHOW DATABASES');
        allSchemas = rows.map(r => r.Database).filter(d => !['information_schema', 'mysql', 'performance_schema', 'sys', 'fixeddata'].includes(d));
        return allSchemas;
    } finally {
        try { if (connection) connection.release(); } catch (e) {}
    }
}

async function execute_set_timedtask_allschemes(dryRun = false) {
    await fillSchemas();
    console.log(chalk.bold.blue("************ TIMEDTASK TOEVOEGEN ALLE ORGANISATIES ************"));

    if (dryRun) {
        console.log(chalk.bgMagenta.white.bold("!!! DRY RUN MODE ACTIVE - GEEN WIJZIGINGEN WORDEN DOORGEVOERD !!!"));
    }

    const pool = getPool();
    const connection = await pool.getConnection();
    const results = [];

    try {
        for (const schema of allSchemas) {
            if (['information_schema', 'mysql', 'performance_schema', 'sys', 'fixeddata'].includes(schema)) continue;

            try {
                const queryCheckExists = `SELECT * FROM ${schema}.timedtask WHERE taskhandle = 'ICY4850HARDWARECHECK'`;
                const [rows] = await connection.query(queryCheckExists);

                if (rows.length > 0) {
                    console.log(chalk.gray(`Check: ${schema} heeft al een hardwarecheck timedtask. Overslaan...`));
                    results.push({ schema, action: 'SKIPPED_EXISTS', executiontime: '' });
                    continue;
                }

                const hour = Math.floor(Math.random() * 3) + 3;
                const minute = Math.floor(Math.random() * 60);
                const executiontime = `${hour.toString().padStart(2, '0')}:${minute.toString().padStart(2, '0')}`;
                const queryAddTimedtask = `INSERT INTO ${schema}.timedtask (taskhandle, category, executioninterval, lastexecuted, executiontime, deadline) VALUES ("ICY4850HARDWARECHECK", 0, 1440, '1970-01-01 00:00:01', '${executiontime}', 60)`;

                if (dryRun) {
                    console.log(chalk.magenta(`[DRY RUN] ${schema} krijgt timedtask op ${executiontime}`));
                    results.push({ schema, action: 'DRY_RUN_ADD', executiontime });
                } else {
                    console.log(chalk.yellow(`*** ${schema} krijgt timedtask op ${executiontime}. Toevoegen... ***`));
                    await connection.query(queryAddTimedtask);
                    results.push({ schema, action: 'ADDED', executiontime });
                }
            } catch (error) {
                if (error && error.code === 'ER_NO_SUCH_TABLE') {
                    console.log(chalk.gray(`************ ${schema} heeft geen timedtask tabel. Overslaan. ************`));
                    results.push({ schema, action: 'SKIPPED_NO_TABLE', executiontime: '' });
                    continue;
                }

                console.error(chalk.red(`Fout bij schema ${schema}: ${error.message || error}`));
                results.push({ schema, action: 'ERROR', executiontime: '', error: error.message || String(error) });
            }
        }
    } finally {
        try { connection.release(); } catch (e) {}
    }

    const addedCount = results.filter(r => r.action === 'ADDED').length;
    const skippedCount = results.filter(r => r.action.startsWith('SKIPPED')).length;
    const errorCount = results.filter(r => r.action === 'ERROR').length;

    console.log(chalk.green(`Klaar. Toegevoegd: ${addedCount} | Overgeslagen: ${skippedCount} | Fouten: ${errorCount}`));
    writeLog(`TIMEDTASK SUMMARY: added=${addedCount}, skipped=${skippedCount}, errors=${errorCount}`);

    return { addedCount, skippedCount, errorCount, results };
}

// Insert helper (adapted from bak/index.js). Uses a direct connection for compatibility with legacy behavior.
async function insertIntoSendlist(scheme, priority = 5, sureness = 1, starttime = "1970-01-01 00:00:01", retrystodo = 5, lasttry = "1970-01-01 00:00:01", comment = '', address, devid, command, msgdata, newpincode = -1, followingid = null) {
    const query_insert_into_sendlist = `INSERT INTO ${scheme}.sendlist (priority, sureness, starttime, retrystodo, lasttry, comment, address, devid, command, msgdata, newpincode, followingid) VALUES (${priority}, ${sureness}, '${starttime}', ${retrystodo}, '${lasttry}', ${mysql.escape(comment)}, ${address}, ${devid}, ${command}, '${msgdata}', ${newpincode}, ${followingid})`;
    const connection = await mysql.createConnection({
        host: dbUrl,
        port: DB_URL_PORT,
        user: dbUsername,
        password: dbPassword,
        ssl: { rejectUnauthorized: false }
    });
    try {
        const [result] = await connection.query(query_insert_into_sendlist);
        await connection.end();
        return result;
    } catch (e) {
        try { await connection.end(); } catch (_) {}
        console.error("************ FOUTEN INSERT INTO SENDLIST FUNCTION  ************");
        throw e;
    }
}

async function execute_change_schakelsettings_4850cm(scheme, dryRun = false) {
    if (dryRun) {
        console.log(chalk.bgMagenta.white.bold("!!! DRY RUN MODE ACTIVE - GEEN WIJZIGINGEN WORDEN DOORGEVOERD !!!"));
    }
    let sendlist_add_failed = [];
    let reportEntries = [];
    const get_all_modules_query = `SELECT * FROM ${scheme}.slavedevice`;
    try {
        // alle modules ophalen
        const connection = await mysql.createConnection({
            host: dbUrl,
            port: DB_URL_PORT,
            user: dbUsername,
            password: dbPassword,
            ssl: { rejectUnauthorized: false }
        });

        const [rows] = await connection.query(get_all_modules_query);

        for (const campere of rows) {
            if (campere.slavedevid !== 8705) {
                console.log(`Address: ${campere.slaveaddress} | DeviceID: ${campere.slavedevid} | Geen ICY4850CM. Overslaan.`);
                continue;
            }

            let report_sendlist_item;
            try {
                let check = true;
                const curconfig = campere.curconfig;
                const curconfig_firstBytes = curconfig.slice(0, 8);
                const wanted_newtimeoff = "3c"; //60 seconden

                const check_time_now = curconfig.slice(8, 10);
                if (check_time_now === "3c" || parseInt(check_time_now, 16) >= 60) {
                    console.log(`Address: ${campere.slaveaddress} | Al (meer dan) 60 seconden schakeltijd ingesteld. Overslaan.`);
                    writeLog(`SKIP module: ${scheme} address=${campere.slaveaddress} current_seconds=${parseInt(check_time_now,16)} (>=60)`);
                    reportEntries.push({ schema: scheme, slaveaddress: campere.slaveaddress, slavedeviceid: campere.slavedeviceid, action: 'SKIPPED', reason: 'already_>=60', old_config: campere.curconfig || '', new_config: '', controller_address: '', controller_devid: '', dryRun });
                    continue;
                }

                const curconfig_lastBytes = curconfig.slice(10, 14);
                const campere_hex_address = campere.slaveaddress.toString(16).padStart(4, '0');
                const slavecommand = "03";
                const controller_command = "3f";

                const sendlist_wanted_config = curconfig_firstBytes + wanted_newtimeoff + curconfig_lastBytes;
                console.log(`Address: ${campere.slaveaddress} | Oud: ${curconfig} | Nieuw: ${sendlist_wanted_config}`);
                writeLog(`WILL CHANGE: ${scheme} address=${campere.slaveaddress} old=${curconfig} new=${sendlist_wanted_config}`);

                console.log(campere);
                let check_wantedconfig_changed = false;
                try {
                    const wantedconfig_firstbytes = (campere.wantedconfig || '').slice(0, 8);
                    const wantedconfig_lastbytes = (campere.wantedconfig || '').slice(10, 14);
                    const new_wantedconfig = wantedconfig_firstbytes + wanted_newtimeoff + wantedconfig_lastbytes;

                    const query_change_wantedconfig = `UPDATE ${scheme}.slavedevice SET wantedconfig = '${new_wantedconfig}' WHERE slaveaddress = ${campere.slaveaddress}`;
                    if (!dryRun) {
                        await connection.query(query_change_wantedconfig);
                        writeLog(`UPDATED wantedconfig for ${scheme} address=${campere.slaveaddress}`);
                        check_wantedconfig_changed = true;
                    } else {
                        console.log(chalk.magenta(`[DRY RUN] Zou uitvoeren: ${query_change_wantedconfig}`));
                        check_wantedconfig_changed = true; // Simulate success
                    }
                } catch (error) {
                    console.error("********** FOUT BIJ AANPASSEN WANTEDCONFIG TABEL SLAVEDEVICE **********");
                    console.error(error);
                }

                let check_unoccupiedconfig_changed = false;
                try {
                    const unoccupiedconfig_firstbytes = (campere.unoccupiedconfig || '').slice(0, 8);
                    const unoccupiedconfig_lastbytes = (campere.unoccupiedconfig || '').slice(10, 14);
                    const new_unoccupiedconfig = unoccupiedconfig_firstbytes + wanted_newtimeoff + unoccupiedconfig_lastbytes;

                    const query_change_unoccupiedconfig = `UPDATE ${scheme}.slavedevice SET unoccupiedconfig = '${new_unoccupiedconfig}' WHERE slaveaddress = ${campere.slaveaddress}`;
                    if (!dryRun) {
                        await connection.query(query_change_unoccupiedconfig);
                        writeLog(`UPDATED unoccupiedconfig for ${scheme} address=${campere.slaveaddress}`);
                        check_unoccupiedconfig_changed = true;
                    } else {
                        console.log(chalk.magenta(`[DRY RUN] Zou uitvoeren: ${query_change_unoccupiedconfig}`));
                        check_unoccupiedconfig_changed = true; // Simulate success
                    }
                } catch (error) {
                    console.error("********** FOUT BIJ AANPASSEN UNOCCUPIEDCONFIG TABEL SLAVEDEVICE **********");
                    console.error(error);
                }

                const query_deviceaddress_controller = `SELECT address,devid FROM ${scheme}.device WHERE deviceid = ${campere.deviceid}`;
                let controller_data = await connection.query(query_deviceaddress_controller);
                let controller_address;
                let controller_devid;
                let new_msgdata;
                if (controller_data[0].length === 0) {
                    console.error("********** GEEN CONTROLLER GEVONDEN VOOR MODULE ADDRESS" + campere.slaveaddress + ". SLAAT OVER! **********");
                    check = false;
                    writeLog(`NO CONTROLLER: ${scheme} address=${campere.slaveaddress}`,'ERROR');
                    reportEntries.push({ schema: scheme, slaveaddress: campere.slaveaddress, slavedeviceid: campere.slavedeviceid, action: 'SKIPPED', reason: 'no_controller', old_config: campere.curconfig || '', new_config: sendlist_wanted_config, controller_address: '', controller_devid: '', dryRun });
                } else {
                    controller_address = controller_data[0][0].address;
                    controller_devid = controller_data[0][0].devid;
                    new_msgdata = campere_hex_address + slavecommand + sendlist_wanted_config;
                }

                report_sendlist_item = {
                    scheme: scheme,
                    slavedeviceid: campere.slavedeviceid,
                    slaveaddress: campere.slaveaddress,
                    old_config: curconfig,
                    new_config: sendlist_wanted_config,
                    controller_address: controller_address,
                    sendlist_add: {
                        controller_address: controller_address,
                        controller_devid: controller_devid,
                        command: controller_command,
                        msgdata: new_msgdata
                    },
                    campere: campere
                };

                if (check === false || check_wantedconfig_changed === false || check_unoccupiedconfig_changed === false) {
                    console.log("Checks failed, niet toevoegen aan sendlist");
                    console.error("************* MODULE STOP | CHECK IS FALSE  ************");
                    sendlist_add_failed.push(report_sendlist_item);
                    writeLog(`NOT ADDED (checks failed): ${scheme} address=${campere.slaveaddress}`,'WARN');
                    reportEntries.push({ schema: scheme, slaveaddress: campere.slaveaddress, slavedeviceid: campere.slavedeviceid, action: 'SKIPPED', reason: 'checks_failed', old_config: campere.curconfig || '', new_config: sendlist_wanted_config, controller_address: controller_address || '', controller_devid: controller_devid || '', dryRun });
                    continue;
                }

                if ((new_msgdata && new_msgdata.length !== 20) || !controller_address || !controller_devid || campere_hex_address.length > 4) check = false;

                if (check === true || check_wantedconfig_changed === true || check_unoccupiedconfig_changed === true) {
                    if (!dryRun) {
                        let result = await insertIntoSendlist(scheme, 30, 1, "1970-01-01 00:00:01", 5, "1970-01-01 00:00:01", "force config campere by icy", parseInt(controller_address, 10), parseInt(controller_devid, 10), parseInt(controller_command, 16), new_msgdata, -1, null);
                        writeLog(`ADDED TO SENDLIST: ${scheme} address=${campere.slaveaddress} controller=${controller_address} devid=${controller_devid}`);
                        reportEntries.push({ schema: scheme, slaveaddress: campere.slaveaddress, slavedeviceid: campere.slavedeviceid, action: 'ADDED', reason: '', old_config: campere.curconfig || '', new_config: sendlist_wanted_config, controller_address: controller_address || '', controller_devid: controller_devid || '', dryRun });
                    } else {
                        console.log(chalk.magenta(`[DRY RUN] Zou uitvoeren: insertIntoSendlist(...) voor module ${campere.slaveaddress}`));
                        writeLog(`[DRY RUN] insertIntoSendlist for ${scheme} address=${campere.slaveaddress}`);
                        reportEntries.push({ schema: scheme, slaveaddress: campere.slaveaddress, slavedeviceid: campere.slavedeviceid, action: 'DRY_RUN_ADDED', reason: '', old_config: campere.curconfig || '', new_config: sendlist_wanted_config, controller_address: controller_address || '', controller_devid: controller_devid || '', dryRun });
                    }
                } else {
                    console.log("Check failed, niet toevoegen aan sendlist");
                    console.error("************* MODULE STOP | CHECK IS FALSE  ************");
                    sendlist_add_failed.push(report_sendlist_item);
                    writeLog(`NOT ADDED (final check failed): ${scheme} address=${campere.slaveaddress}`,'WARN');
                    reportEntries.push({ schema: scheme, slaveaddress: campere.slaveaddress, slavedeviceid: campere.slavedeviceid, action: 'SKIPPED', reason: 'final_check_failed', old_config: campere.curconfig || '', new_config: sendlist_wanted_config, controller_address: controller_address || '', controller_devid: controller_devid || '', dryRun });
                }

            } catch (e) {
                sendlist_add_failed.push(report_sendlist_item);
                console.log("ERROR: " + e.message);
                console.log(e);
                writeLog(`ERROR processing module ${scheme} address=${report_sendlist_item?.slaveaddress || 'unknown'}: ${e.message || e}`,'ERROR');
                reportEntries.push({ schema: scheme, slaveaddress: report_sendlist_item?.slaveaddress || '', slavedeviceid: report_sendlist_item?.slavedeviceid || '', action: 'ERROR', reason: e.message || String(e), old_config: report_sendlist_item?.old_config || '', new_config: report_sendlist_item?.new_config || '', controller_address: report_sendlist_item?.sendlist_add?.controller_address || '', controller_devid: report_sendlist_item?.sendlist_add?.controller_devid || '', dryRun });
                continue;
            }
        }
        console.log("************ KLAAR. ************");
        writeLog(`FINISHED: execute_change_schakelsettings_4850cm for schema=${scheme} failed_count=${sendlist_add_failed.length}`);
        // Write CSV/JSON report
        try {
            const report_ts = new Date().toISOString().slice(0,19).replace(/[:T]/g,'-');
            const reportFileCsv = path.join(EXPORT_DIR, `icy4850_changes_${scheme}_${report_ts}.csv`);
            writeCsvReport(reportFileCsv, reportEntries);
            writeLog(`WROTE REPORT: ${reportFileCsv}`);
        } catch (e) {
            writeLog(`ERROR WRITING REPORT: ${e.message || e}`,'ERROR');
        }
        if (sendlist_add_failed.length === 0) {
            console.log("Voor alle modules is een opdracht in sendlist toegevoegd.")
        } else {
            console.log(sendlist_add_failed);
            console.log("BOVENSTAANDE WERDEN UIT VEILIGHEID NIET TOEGEVOEGD AAN DE SENDLIST.");
            console.log(`Aantal: ${sendlist_add_failed.length}`);
        }
        console.log("****** BEËINDIGD ******");
        try { await connection.end(); } catch (e) {}
        return { failed_count: sendlist_add_failed.length };

    } catch (e) {
        console.error("************ FOUTEN MET CONNECTIE DATABASE ************");
        console.log(e.message);
        console.log("************** BEËINDIGD *************");
        writeLog(`ERROR schema-level: ${scheme} - ${e.message || e}`,'ERROR');
        return { error: e.message || String(e) };
    }
}

async function uitdraai_schakeltijden_4850() {
    try {
        await fillSchemas();
        const pool = getPool();
        const connection = await pool.getConnection();

        let to_report = [];
        for (let schema of allSchemas) {
            try {
                const [rows] = await connection.query(`SELECT * FROM ${schema}.slavedevice WHERE slavedevid = 8705`);

                let index_scanned_modules = 0;
                let controle_array = [];
                for (const row of rows) {
                    index_scanned_modules++;
                    const schakeltijd_hex = row.curconfig.slice(8, 10);
                    let schakeltijd_dec = parseInt(schakeltijd_hex, 16);
                    if (schakeltijd_dec < 60) {
                        controle_array.push(schakeltijd_dec);
                    }
                }

                if (controle_array.length > 0) {
                    const laagste_seconden = Math.min(...controle_array);
                    const hoogste_seconden = Math.max(...controle_array);
                    console.log(chalk.magenta(`${schema}: Laagste seconden: ${laagste_seconden} | Hoogste seconden: ${hoogste_seconden} |`));

                    to_report.push({
                        schema: schema,
                        Laagste_seconden: laagste_seconden,
                        Hoogste_seconden: hoogste_seconden,
                        scanned_modules: index_scanned_modules
                    });
                }

            } catch (error) {
                if (error.code === "ER_NO_SUCH_TABLE") {
                    continue;
                }
                console.error(`Error`, error);
            }
        }

        try { connection.release(); } catch (e) {}

        // Maak een excel bestand en sla resultaat op
        const ws = XLSX.utils.json_to_sheet(to_report);

        // Headers bold maken en styling toepassen
        if (to_report.length > 0) {
            const range = XLSX.utils.decode_range(ws['!ref']);
            for (let col = range.s.c; col <= range.e.c; col++) {
                const cellAddress = XLSX.utils.encode_cell({ r: 0, c: col });
                if (!ws[cellAddress]) continue;
                ws[cellAddress].s = {
                    font: { bold: true, sz: 11 },
                    alignment: { vertical: 'center', horizontal: 'left' }
                };
            }
        }

        // AutoFilter toevoegen
        if (to_report.length > 0) {
            const range = XLSX.utils.decode_range(ws['!ref']);
            ws['!autofilter'] = { ref: XLSX.utils.encode_range(range) };

            // Kolombreedtes instellen
            ws['!cols'] = [
                { wch: 30 }, // schema
                { wch: 20 }, // Laagste_seconden
                { wch: 20 }, // Hoogste_seconden
                { wch: 20 }  // scanned_modules
            ];
        }

        const wb = XLSX.utils.book_new();
        XLSX.utils.book_append_sheet(wb, ws, "Schakeltijden 4850CM");
        const filename = `icy4850cm_schakeltijden_rapport_${new Date().toISOString().slice(0, 19).replace(/[:T]/g, '-')}.xlsx`;
        const fullPath = path.join(EXPORT_DIR, filename);
        XLSX.writeFile(wb, fullPath, { cellStyles: true });
        console.log(`RAPPORTAGE OPSLAAN GELUKT: ${fullPath}`);

        const openAnswer = await inquirer.prompt([
            { type: 'confirm', name: 'open', message: 'Bestand openen?', default: true }
        ]);

        if (openAnswer.open) {
            await openFile(fullPath);
        }
    } catch (error) {
        console.log("Error", error);
    }
    console.log("=-=-=-=-=-=-=-=-=-=-=-=-=--=-=-=-=-=-=-=-=");
    console.log("Bovenstaande parken hebben tenminste 1 module met een schakeltijd onder de 60 seconden.");
    console.log("Parken met meer dan 60 seconden schakeltijd worden niet gerapporteerd.");
    console.log("************ KLAAR ************");
    process.exit(0);
}

async function execute_enabled_check() {
    await fillSchemas();
    try {
        const pool = getPool();
        const connection = await pool.getConnection();
        try {
            for (let schema of allSchemas) {


            if (schema === 'information_schema' || schema === 'mysql' || schema === 'performance_schema' || schema === 'sys') {
                continue;
            }

            //Check if already category ICY4850HARDWARECHECK id ENABLE exitst in schema.settings
            try {

                const query_check_exists = `SELECT * FROM ${schema}.settings WHERE category = 'ICY4850HARDWARECHECK' AND id = 'ENABLE'`;
                const [rows] = await connection.query(query_check_exists);
                if (rows.length > 0) {
                    if (rows[0].value === "true") {
                        console.log(chalk.green(`${schema}: High Resolution Measurement Hardwarecheck is INGESCHAKELD.`));
                    } else {
                        console.log(chalk.red(`${schema}: High Resolution Measurement Hardwarecheck is UITGESCHAKELD.`));
                    }
                } else {
                    console.log(chalk.yellow(`${schema}: Heeft nog geen ICY4850HARDWARECHECK settings.`));
                }
            } catch (e) {
                console.error(chalk.red(`!!!!!!!!!!!!!!!! FOUT BIJ SCHEMA ${schema}: ${e.message} !!!!!!!!!!!!!!!!`));
            }
            }
        } finally {
            try { connection.release(); } catch (e) {}
        }
    } catch (error) {
        console.error(error);
        console.error("!!!!!!!!!!!!!!! NOODSTOP !!!!!!!!!!!!!!!!!!!");
        process.exit(1);
    }
    console.log("************ KLAAR ************")
    process.exit(0);
}

// Scan alle schemas op modules met schakeltijd < 60 seconden
async function execute_check_and_offer_convert_schakeltijden_all(interactive = true) {
    await fillSchemas();
    console.log(chalk.bold.blue('************ CHECK SCHAKELTIJDEN (ALLE ORGANISATIES) ************'));
    const pool = getPool();
    const connection = await pool.getConnection();
    const schemasWithShortTimes = [];
    try {
        for (const schema of allSchemas) {
        if (['information_schema', 'mysql', 'performance_schema', 'sys'].includes(schema)) continue;
        try {
            const [rows] = await connection.query(`SELECT slaveaddress, slavedevid, curconfig, slavedeviceid FROM ${schema}.slavedevice WHERE slavedevid = 8705`);
            const problematic = [];
            for (const r of rows) {
                try {
                    if (!r.curconfig || r.curconfig.length < 10) continue;
                    const hex = r.curconfig.slice(8, 10);
                    const sec = parseInt(hex, 16);
                    if (!Number.isNaN(sec) && sec < 60) {
                        problematic.push({ slaveaddress: r.slaveaddress, seconds: sec, slavedeviceid: r.slavedeviceid });
                    }
                } catch (e) {
                    continue;
                }
            }
            if (problematic.length > 0) {
                schemasWithShortTimes.push({ schema, count: problematic.length, details: problematic });
                console.log(chalk.yellow(`${schema}: ${problematic.length} module(s) met schakeltijd < 60s`));
                writeLog(`SCAN: ${schema} has ${problematic.length} modules with schakeltijd < 60s`);
            } else {
                console.log(chalk.gray(`${schema}: geen modules met schakeltijd < 60s`));
            }
        } catch (e) {
            if (e.code === 'ER_NO_SUCH_TABLE') continue;
            console.error(chalk.red(`Fout bij schema ${schema}: ${e.message}`));
        }
        }
    } finally {
        try { connection.release(); } catch (e) {}
    }

    if (schemasWithShortTimes.length === 0) {
        console.log(chalk.green('Geen organisaties gevonden met modules met schakeltijd < 60 seconden.'));
        if (interactive) process.exit(0);
        return;
    }

    writeLog(`SCAN SUMMARY: found ${schemasWithShortTimes.length} schemas with short times: ${schemasWithShortTimes.map(s=>s.schema).join(', ')}`);

    // Bied optie om scan-resultaten te exporteren naar Excel
    try {
        const exportAnswer = await inquirer.prompt([{ type: 'confirm', name: 'export', message: 'Resultaten exporteren naar Excel?', default: true }]);
        if (exportAnswer.export) {
            const rows = [];
            for (const s of schemasWithShortTimes) {
                for (const d of s.details) {
                    rows.push({ Schema: s.schema, SlaveAddress: d.slaveaddress, Seconds: d.seconds, Slavedeviceid: d.slavedeviceid });
                }
            }

            const ws = XLSX.utils.json_to_sheet(rows);
            if (rows.length > 0 && ws['!ref']) {
                const range = XLSX.utils.decode_range(ws['!ref']);
                for (let col = range.s.c; col <= range.e.c; col++) {
                    const cellAddress = XLSX.utils.encode_cell({ r: 0, c: col });
                    if (!ws[cellAddress]) continue;
                    ws[cellAddress].s = { font: { bold: true, sz: 11 }, alignment: { vertical: 'center', horizontal: 'left' } };
                }
                ws['!autofilter'] = { ref: XLSX.utils.encode_range(range) };
            }

            const wb = XLSX.utils.book_new();
            XLSX.utils.book_append_sheet(wb, ws, 'ScanResultaten');
            const filename = `icy4850_schakeltijden_scan_${new Date().toISOString().slice(0, 19).replace(/[:T]/g, '-')}.xlsx`;
            const fullPath = path.join(EXPORT_DIR, filename);
            XLSX.writeFile(wb, fullPath, { cellStyles: true });
            console.log(`RAPPORTAGE OPSLAAN GELUKT: ${fullPath}`);

            const openAnswer = await inquirer.prompt([{ type: 'confirm', name: 'open', message: 'Bestand openen?', default: true }]);
            if (openAnswer.open) await openFile(fullPath);
        }
    } catch (e) {
        console.error(chalk.red('Fout bij exporteren:'), e.message || e);
    }

    // Vraag of de gebruiker een compacte statistiek-export wil (aantal parken met modules <60s, swversions)
    try {
        const statsAnswer = await inquirer.prompt([{ type: 'confirm', name: 'exportStats', message: 'Export stats (unieke schemas met aantal modules <60s en swversion)?', default: false }]);
        if (statsAnswer.exportStats) {
            await export_schakeltijden_under60_stats(true);
        }
    } catch (e) {
        // ignore
    }

    // Interactive: bied keuze om te converteren
    if (interactive) {
        const choices = schemasWithShortTimes.map(s => ({ name: `${s.schema} (${s.count} modules)`, value: s.schema }));
        const answer = await inquirer.prompt([
            { type: 'checkbox', name: 'selected', message: 'Kies organisatie(s) om modules om te zetten naar 60 seconden (via sendlist):', choices: choices, pageSize: INQUIRER_PAGE_SIZE }
        ]);

        if (!answer.selected || answer.selected.length === 0) {
            console.log(chalk.yellow('Geen organisaties geselecteerd. Beëindigen.'));
            process.exit(0);
        }

        const dryRunAnswer = await inquirer.prompt([
            { type: 'list', name: 'mode', message: 'Kies uitvoermodus:', choices: [ { name: 'Dry Run (simulatie)', value: true }, { name: 'Live (voer wijzigingen uit)', value: false } ], pageSize: INQUIRER_PAGE_SIZE }
        ]);

        console.log(`Geselecteerde organisaties: ${answer.selected.join(', ')}`);
        writeLog(`USER SELECTED: ${answer.selected.join(', ')} modeDryRun=${dryRunAnswer.mode}`);
        if (dryRunAnswer.mode) console.log(chalk.magenta('MODUS: DRY RUN (geen wijzigingen)'));

        const confirm = await inquirer.prompt([{ type: 'confirm', name: 'ok', message: 'Weet je zeker dat je wilt doorgaan?', default: false }]);
        if (!confirm.ok) process.exit(0);

        for (const sch of answer.selected) {
            console.log(chalk.blue(`-- Uitvoeren voor schema: ${sch} --`));
            try {
                const res = await execute_change_schakelsettings_4850cm(sch, dryRunAnswer.mode);
                if (res && res.error) {
                    console.error(chalk.red(`Fout bij verwerken ${sch}: ${res.error}`));
                    writeLog(`SKIP SCHEMA: ${sch} - ${res.error}`,'ERROR');
                } else {
                    writeLog(`DONE SCHEMA: ${sch} - result=${JSON.stringify(res)}`);
                }
            } catch (e) {
                console.error(chalk.red(`Fout bij verwerken ${sch}: ${e.message || e}`));
                writeLog(`EXCEPTION SCHEMA: ${sch} - ${e.message || e}`,'ERROR');
            }
        }
    }
}

// Add standard settings to all schemas (with dryRun support)
async function execute_add_settings(dryRun = false) {
    await fillSchemas();
    console.log(chalk.bold.blue('************ SETTINGS TOEVOEGEN AAN ALLE ORGANISATIES ************'));
    console.log(chalk.yellow('Controleren of setting ICY4850HARDWARECHECK (ENABLE) aanwezig is per schema.'));

    try {
        const pool = getPool();
        const connection = await pool.getConnection();
        try {
            for (const schema of allSchemas) {
            if (['information_schema', 'mysql', 'performance_schema', 'sys'].includes(schema)) continue;
            try {
                const query_check = `SELECT * FROM ${schema}.settings WHERE category = 'ICY4850HARDWARECHECK' AND id = 'ENABLE' LIMIT 1`;
                const [rows] = await connection.query(query_check);
                if (rows.length > 0) {
                    console.log(chalk.gray(`${schema}: setting bestaat al. Overslaan.`));
                    continue;
                }

                const insertQuery = `INSERT INTO ${schema}.settings ( category, id, value )\n                                        VALUES\n                                        ( 'ICY4850HARDWARECHECK', 'ENABLE',             'true'          ),  -- enable ICY4850 hardware check\n                                        ( 'ICY4850HARDWARECHECK', 'MAX_P',              '0.55'           ),  -- in W\n                                        ( 'ICY4850HARDWARECHECK', 'MAX_P_SURE_OFF',     '0.55'           ),  -- in W above this is defect\n                                        ( 'ICY4850HARDWARECHECK', 'MIN_I',              '0.2'            ),  -- in A\n                                        ( 'ICY4850HARDWARECHECK', 'MINLOADFORCHECK',    '10.0'           ),  -- min load for check (used in timedtask) in W\n                                        ( 'ICY4850HARDWARECHECK', 'UNRELIABLEMAIL',     'false'          );`;
                if (dryRun) {
                    console.log(chalk.magenta(`[DRY RUN] ${schema}: zou uitvoeren: ${insertQuery}`));
                } else {
                    try {
                        await connection.query(insertQuery);
                        console.log(chalk.green(`${schema}: setting(s) toegevoegd.`));
                    } catch (e) {
                        if (e.code === 'ER_NO_SUCH_TABLE') {
                            console.log(chalk.yellow(`${schema}: heeft geen 'settings' tabel. Overslaan.`));
                        } else {
                            console.log(chalk.red(`${schema}: fout bij toevoegen setting(s): ${e.message}`));
                        }
                    }
                }

            } catch (e) {
                if (e.code === 'ER_NO_SUCH_TABLE') {
                    console.log(chalk.yellow(`${schema}: heeft geen \'settings\' tabel. Overslaan.`));
                    continue;
                }
                console.log(chalk.red(`${schema}: fout bij controle: ${e.message}`));
            }
            }
        } finally {
            try { connection.release(); } catch (e) {}
        }
    } catch (err) {
        console.error(chalk.red('Fout bij verbinden met database: ' + err.message));
        process.exit(1);
    }

    console.log(chalk.bold.green('************ KLAAR ************'));
    process.exit(0);
}

function formatTimestamp(ts) {
    // Zorg dat ts een Date object is
    const d = new Date(ts);

    const yyyy = d.getFullYear();
    const mm = String(d.getMonth() + 1).padStart(2, '0');
    const dd = String(d.getDate()).padStart(2, '0');
    const hh = String(d.getHours()).padStart(2, '0');
    const min = String(d.getMinutes()).padStart(2, '0');
    const ss = String(d.getSeconds()).padStart(2, '0');

    return `${yyyy}-${mm}-${dd} ${hh}-${min}-${ss}`;
}

function openFile(filePath) {
    return new Promise((resolve) => {
        const command = process.platform === 'win32' ? `start "" "${filePath}"` : `open "${filePath}"`;
        require('child_process').exec(command, (error) => {
            if (error) {
                console.error("Fout bij openen bestand:", error);
            }
            // We wachten kort om zeker te zijn dat het commando is afgevuurd
            setTimeout(resolve, 500);
        });
    });
}

let to_report = [];
async function get_status_per_park() {
    //We halen alle parken op
    await fillSchemas();
    const pool = getPool();
    const connection = await pool.getConnection();

    let i = 0;
    console.log(`***** Checks uitvoeren alle parken *****`);
    for (const scheme of allSchemas) {
        i++
        to_report.push({ schema: scheme, issues: [] });

        const sql_get_sd = `SELECT * FROM ${scheme}.icy4850hardwareissue WHERE state != "STATUS_OK"`;
        // console.log(sql_get_sd);

        //We gaan dit uitvoeren
        try {
            let already_checked = [];
            const [rows] = await connection.query(sql_get_sd);
            // console.log(rows);
            for (const meting of rows) {
                if (already_checked.includes(meting.slaveaddress)) {
                    continue;
                } else {
                    already_checked.push(meting.slaveaddress);
                }

                const sql_get_last_measurement = `SELECT * FROM ${scheme}.icy4850hardwareissue WHERE slaveaddress = ${meting.slaveaddress} ORDER BY timestamp DESC LIMIT 1`;
                let [last_measurement] = await connection.query(sql_get_last_measurement);
                last_measurement = last_measurement[0];
                // console.log(last_measurement);

                const sql_check_replaced = `SELECT * FROM ${scheme}.slavedevice WHERE slaveaddress = ${meting.slaveaddress}`;
                let [check_replaced] = await connection.query(sql_check_replaced);
                if (check_replaced.length === 0) {
                    continue; //Module is vervangen, niet rapporteren.
                }

                // Derive software revision and replacement guidance (structural addition from Merijn)
                const sw_revisie_raw = check_replaced[0].swversion ? check_replaced[0].swversion.slice(4, 6) : null;
                const sw_revisie = sw_revisie_raw ? parseInt(sw_revisie_raw, 10) : null;
                let vervang_status = "Fout Onbekende status";

                if (sw_revisie !== null) {
                    if (sw_revisie < 18) {
                        // Skip reporting for very old revisions per Merijn's rules
                        continue;
                    }
                    if (sw_revisie < 20 && last_measurement.state === "STATUS_DEFECT") {
                        vervang_status = "Wantrouwen, versie oud Mogelijk wel in KWH modus!";
                    } else if (sw_revisie >= 20 && last_measurement.state === "STATUS_DEFECT") {
                        vervang_status = "VERVANGEN ZSM";
                    } else if (sw_revisie < 20 && (last_measurement.state === "STATUS_UNRELIABLE" || last_measurement.state === null)) {
                        vervang_status = "Wantrouwen, versie oud, wachten op timedtask. Mogelijk wel in KWH Modus!";
                    } else if (sw_revisie >= 20 && (last_measurement.state === "STATUS_UNRELIABLE" || last_measurement.state === null)) {
                        vervang_status = "VERDACHT, wachten op timedtask.";
                    }
                }

                if (last_measurement.state === "STATUS_OK") {
                    continue; //Laatste meting is oke, rapporteren we niet.
                } else {
                    let report_object = {
                        scheme: scheme,
                        address: last_measurement.slaveaddress,
                        state_now: last_measurement.state,
                        highresolutionmeasurement_last: last_measurement,
                        highresolutionmeasurement_first: meting,
                        sw_revisie: sw_revisie,
                        vervang_status: vervang_status
                    }
                    to_report[i - 1].issues.push(report_object); //Rapporteer deze module voor de organisatie

                    const msg = `${scheme} | Address: ${last_measurement.slaveaddress} | STATUS: ${last_measurement.state} | Current: ${last_measurement.currentrms}A | Power: ${last_measurement.activepower}W | Timestamp: ${last_measurement.timestamp}`;
                    switch (last_measurement.state) {
                        case "STATUS_UNRELIABLE":
                            console.log(chalk.yellow(`[VERDACHT] ${msg}`));
                            break;
                        case "STATUS_DEFECT":
                            console.log(chalk.red(`[DEFECT] ${msg}`));
                            break;
                        case null:
                            console.log(chalk.blue(`[PREMATURE] ${msg}`));
                            break;
                        default:
                            console.log(chalk.gray(`[ONBEKEND] ${msg}`));
                            break;
                    }
                }

            }

        } catch (e) {
            if (e.code !== "ER_NO_SUCH_TABLE") {
                console.error(chalk.red(`FOUT BIJ OPHALEN MODULES VAN SCHEMA ${scheme}: ${e.message}`));
            }
            continue;
        }
        // break;
    }
    // try {
    //     connection.close();
    //     console.log("************ KLAAR ************");

    //     let wsr = "";
    //     let report_time = new Date()
    //         .toISOString()
    //         .slice(0, 19)        // yyyy-mm-ddThh:mm:ss
    //         .replace("T", " ")   // yyyy-mm-dd hh:mm:ss
    //         .replace(/:/g, "-"); // yyyy-mm-dd hh-mm-ss

    //     console.log(report_time);
    //     wsr += `ICY4850CM Hardware Issue Check Rapport (High Resolution Measurement)\n`;
    //     wsr += `Rapportage tijd: ${report_time}\n`;
    //     wsr += `Database: ${dbUrl}\n`;
    //     wsr += `Rapporteert alle modules die **NU** verdacht of defect zijn van alle organisaties en die nog NIET vervangen zijn.\n`;
    //     wsr += `Een status null is een premature check.\n`;

    //     for (const report of to_report) {
    //         if (report.issues.length === 0) continue;
    //         //address naar HEX:
    //         wsr += `\n`;
    //         wsr += `Schema: ${report.schema}`;

    //         for (const issue of report.issues) {
    //             //address naar HEX:
    //             const address_hex = issue.address.toString(16).padStart(4, '0').toUpperCase();
    //             wsr += `\n   Address: ${issue.address} (${address_hex}): HUIDIGE STATUS: ${issue.state_now}\n`;
    //             wsr += `      << First measurement: Timestamp: ${formatTimestamp(issue.highresolutionmeasurement_first.timestamp)}: Current: ${issue.highresolutionmeasurement_first.currentrms}A: Power: ${issue.highresolutionmeasurement_first.activepower}W: Status meting: ${issue.highresolutionmeasurement_first.state}\n`;
    //             wsr += `      >> Last measurement: Timestamp: ${formatTimestamp(issue.highresolutionmeasurement_last.timestamp)}: Current: ${issue.highresolutionmeasurement_last.currentrms}A: Power: ${issue.highresolutionmeasurement_last.activepower}W: Status meting: ${issue.highresolutionmeasurement_last.state}\n`;
    //         }
    //     }

    //     const filename = `icy4850_hrm_rapport_${new Date().toISOString().slice(0, 19).replace(/[:T]/g, '-')}.csv`;

    //     await fs.promises.writeFile(filename, wsr, "utf8");

    //     console.log(`RAPPORTAGE OPSLAAN GELUKT: ${filename}`);
    // } catch (error) {
    //     console.log("************* FOUT BIJ OPSLAAN ************");
    //     console.log(error);
    // }
    try {
        connection.close();
        console.log(chalk.green("************ KLAAR ************"));

        const report_time = new Date()
            .toISOString()
            .slice(0, 19)
            .replace(/[:T]/g, "-");

        const rows = [];

        for (const report of to_report) {
            if (report.issues.length === 0) continue;

            for (const issue of report.issues) {
                const address_hex = issue.address
                    .toString(16)
                    .padStart(4, "0")
                    .toUpperCase();

                rows.push({
                    "Rapportage tijd": report_time,
                    Schema: report.schema,
                    Address: `${issue.address} (${address_hex})`,
                    "State now": issue.state_now || "N/A",
                    "First Timestamp": issue.highresolutionmeasurement_first?.timestamp ? formatTimestamp(issue.highresolutionmeasurement_first.timestamp) : "N/A",
                    "First Current": issue.highresolutionmeasurement_first?.currentrms ?? "N/A",
                    "First Power": issue.highresolutionmeasurement_first?.activepower ?? "N/A",
                    "First State": issue.highresolutionmeasurement_first?.state ?? "N/A",
                    "Last Timestamp": issue.highresolutionmeasurement_last?.timestamp ? formatTimestamp(issue.highresolutionmeasurement_last.timestamp) : "N/A",
                    "Last Current": issue.highresolutionmeasurement_last?.currentrms ?? "N/A",
                    "Last Power": issue.highresolutionmeasurement_last?.activepower ?? "N/A",
                    "Last State": issue.highresolutionmeasurement_last?.state ?? "N/A",
                    "SW_Revisie": issue.sw_revisie,
                    "Vervang Status": issue.vervang_status || "N/A",
                });
            }
        }

        // Zet data om naar een werkblad
        const ws = XLSX.utils.json_to_sheet(rows);

        // Headers bold maken
        if (rows.length > 0) {
            const range = XLSX.utils.decode_range(ws['!ref']);
            for (let col = range.s.c; col <= range.e.c; col++) {
                const cellAddress = XLSX.utils.encode_cell({ r: 0, c: col });
                if (!ws[cellAddress]) continue;
                ws[cellAddress].s = {
                    font: { bold: true, sz: 11 },
                    alignment: { vertical: 'center', horizontal: 'left' }
                };
            }
        }

        // AutoFilter toevoegen (zoals in screenshot)
        if (rows.length > 0) {
            const range = XLSX.utils.decode_range(ws['!ref']);
            ws['!autofilter'] = { ref: XLSX.utils.encode_range(range) };

            // Kolombreedtes instellen voor betere leesbaarheid
            ws['!cols'] = [
                { wch: 22 }, // Rapportage_tijd
                { wch: 25 }, // Schema
                { wch: 18 }, // Address
                { wch: 20 }, // State_now
                { wch: 22 }, // First_Timestamp
                { wch: 15 }, // First_Current
                { wch: 15 }, // First_Power
                { wch: 20 }, // First_State
                { wch: 22 }, // Last_Timestamp
                { wch: 15 }, // Last_Current
                { wch: 15 }, // Last_Power
                { wch: 20 }  // Last_State
            ];
        }

        // Maak een nieuwe workbook
        const wb = XLSX.utils.book_new();
        XLSX.utils.book_append_sheet(wb, ws, "Rapport");

        // Schrijf naar bestand MET cellStyles optie
        const filename = `icy4850_hrm_rapport_${report_time}.xlsx`;
        const fullPath = path.join(EXPORT_DIR, filename);
        XLSX.writeFile(wb, fullPath, { cellStyles: true });

        console.log(`RAPPORTAGE OPSLAAN GELUKT: ${fullPath}`);

        const openAnswer = await inquirer.prompt([
            { type: 'confirm', name: 'open', message: 'Bestand openen?', default: true }
        ]);

        if (openAnswer.open) {
            await openFile(fullPath);
        }

    } catch (error) {
        console.log("************* FOUT BIJ OPSLAAN ************");
        console.log(error);
    }
    console.log(chalk.green("****** BEËINDIGD ******"));
    process.exit(0);
}

// Search for a decimal address across all schemas and return occurrences per table
async function findAddressInAllSchemas(addressDecimal) {
    if (typeof addressDecimal === 'undefined' || addressDecimal === null) throw new Error('addressDecimal is required');
    await fillSchemas();
    const pool = getPool();
    const connection = await pool.getConnection();
    const results = [];
    writeLog(`START ADDRESS SEARCH: ${addressDecimal} schemas=${allSchemas.length}`);
    try { console.log(chalk.magenta(`Starting address search for ${addressDecimal} across ${allSchemas.length} schemas...`)); } catch(e) {}
    try {
        for (const schema of allSchemas) {
            if (['information_schema', 'mysql', 'performance_schema', 'sys', 'fixeddata'].includes(schema)) continue;
            try { writeLog(`Inspecting schema: ${schema}`); } catch(e) {}
            try { console.log(chalk.gray(`Inspecting ${schema}`)); } catch(e) {}
            try {
                // gather device rows and slavedevice rows for this address using multiple representations
                const hexUpper = addressDecimal.toString(16).toUpperCase();
                const hexLower = hexUpper.toLowerCase();
                const hexWith0x = '0x' + hexUpper;

                let deviceRows = [];
                let slaveRows = [];

                // attempt numeric match
                try {
                    const [dr] = await connection.query(`SELECT * FROM \`${schema}\`.\`device\` WHERE address = ? LIMIT 50`, [addressDecimal]);
                    if (dr && dr.length) deviceRows = dr;
                } catch (e) { /* ignore */ }
                try {
                    const [sr] = await connection.query(`SELECT * FROM \`${schema}\`.\`slavedevice\` WHERE address = ? LIMIT 50`, [addressDecimal]);
                    if (sr && sr.length) slaveRows = sr;
                } catch (e) { /* ignore */ }

                // if none found yet, try hex string forms
                if (!deviceRows.length) {
                    try {
                        const [dr2] = await connection.query(`SELECT * FROM \`${schema}\`.\`device\` WHERE LOWER(address) = ? LIMIT 50`, [hexLower]);
                        if (dr2 && dr2.length) deviceRows = dr2;
                    } catch (e) {}
                }
                if (!slaveRows.length) {
                    try {
                        const [sr2] = await connection.query(`SELECT * FROM \`${schema}\`.\`slavedevice\` WHERE LOWER(address) = ? LIMIT 50`, [hexLower]);
                        if (sr2 && sr2.length) slaveRows = sr2;
                    } catch (e) {}
                }

                if (!deviceRows.length) {
                    try {
                        const [dr3] = await connection.query(`SELECT * FROM \`${schema}\`.\`device\` WHERE LOWER(address) = ? LIMIT 50`, [hexWith0x.toLowerCase()]);
                        if (dr3 && dr3.length) deviceRows = dr3;
                    } catch (e) {}
                }
                if (!slaveRows.length) {
                    try {
                        const [sr3] = await connection.query(`SELECT * FROM \`${schema}\`.\`slavedevice\` WHERE LOWER(address) = ? LIMIT 50`, [hexWith0x.toLowerCase()]);
                        if (sr3 && sr3.length) slaveRows = sr3;
                    } catch (e) {}
                }

                if (!deviceRows.length) {
                    try {
                        const [dr4] = await connection.query(`SELECT * FROM \`${schema}\`.\`device\` WHERE HEX(address) = ? LIMIT 50`, [hexUpper]);
                        if (dr4 && dr4.length) deviceRows = dr4;
                    } catch (e) {}
                }
                if (!slaveRows.length) {
                    try {
                        const [sr4] = await connection.query(`SELECT * FROM \`${schema}\`.\`slavedevice\` WHERE HEX(address) = ? LIMIT 50`, [hexUpper]);
                        if (sr4 && sr4.length) slaveRows = sr4;
                    } catch (e) {}
                }

                // derive type sets
                const deviceTypes = new Set();
                for (const r of deviceRows) {
                    if (typeof r.devicetypeid !== 'undefined' && r.devicetypeid !== null) deviceTypes.add(String(r.devicetypeid));
                }
                const slaveTypes = new Set();
                for (const r of slaveRows) {
                    // try common candidate fields for slave device type
                    if (typeof r.slavedevicetypeid !== 'undefined' && r.slavedevicetypeid !== null) slaveTypes.add(String(r.slavedevicetypeid));
                    else if (typeof r.slavedevid !== 'undefined' && r.slavedevid !== null) slaveTypes.add(String(r.slavedevid));
                    else if (typeof r.devicetypeid !== 'undefined' && r.devicetypeid !== null) slaveTypes.add(String(r.devicetypeid));
                }

                // check for intersection; accept device-only or slave-only matches too
                let matchedType = null;
                if (deviceTypes.size > 0 && slaveTypes.size > 0) {
                    for (const t of deviceTypes) {
                        if (slaveTypes.has(t)) { matchedType = t; break; }
                    }
                    if (matchedType === null) {
                        // no exact intersection; prefer a device type if available, warn
                        matchedType = Array.from(deviceTypes)[0] || Array.from(slaveTypes)[0] || null;
                        try { writeLog(`Type mismatch in ${schema}; accepting representative type ${matchedType}`,'WARN'); } catch(e) {}
                    }
                } else if (deviceTypes.size > 0) {
                    matchedType = Array.from(deviceTypes)[0];
                } else if (slaveTypes.size > 0) {
                    matchedType = Array.from(slaveTypes)[0];
                } else {
                    // no devicetype info available, skip
                    try { writeLog(`No device-type info in ${schema}; skipping`); } catch(e) {}
                    continue;
                }

                // count rows
                const count = (deviceRows.length || 0) + (slaveRows.length || 0);
                results.push({ schema, table: 'device/slavedevice', count, deviceTypes: Array.from(deviceTypes), slaveTypes: Array.from(slaveTypes), matchedType, deviceSample: deviceRows.slice(0,5), slaveSample: slaveRows.slice(0,5) });
                try { writeLog(`MATCH in ${schema}: count=${count}, matchedType=${matchedType}`); } catch(e) {}

            } catch (e) {
                writeLog(`ERROR inspecting ${schema}: ${e.message || e}`, 'WARN');
                console.error(chalk.red(`Error inspecting ${schema}: ${e && e.message ? e.message : e}`));
                continue;
            }
        }
    } finally {
        try { connection.release(); } catch (e) {}
    }
    try { writeLog(`END ADDRESS SEARCH: ${addressDecimal} results=${results.length}`); } catch(e) {}

    if (results.length === 0) {
        console.log(chalk.yellow(`Address ${addressDecimal} not found in any schema tables with an 'address' column and matching device type.`));
    } else {
        console.log(chalk.green(`Found address ${addressDecimal} in ${results.length} place(s) (type-matched):`));
        for (const r of results) {
            console.log(chalk.cyan(`${r.schema}: ${r.count} row(s)  matchedType=${r.matchedType}`));
            if (r.deviceTypes && r.deviceTypes.length) console.log(chalk.gray(' deviceTypes:'), r.deviceTypes.join(','));
            if (r.slaveTypes && r.slaveTypes.length) console.log(chalk.gray(' slaveTypes:'), r.slaveTypes.join(','));
            if (r.deviceSample && r.deviceSample.length) {
                try { console.table(r.deviceSample); } catch (e) { console.log(r.deviceSample); }
            }
            if (r.slaveSample && r.slaveSample.length) {
                try { console.table(r.slaveSample); } catch (e) { console.log(r.slaveSample); }
            }
        }
    }

    // Also write a CSV report to EXPORT_DIR (columns: schema,count,matchedType,deviceTypes,slaveTypes,deviceSample,slaveSample)
    try {
        if (results.length > 0) {
            const rows = [];
            for (const r of results) {
                rows.push({
                    schema: r.schema,
                    count: r.count,
                    matchedType: r.matchedType,
                    deviceTypes: (r.deviceTypes || []).join(','),
                    slaveTypes: (r.slaveTypes || []).join(','),
                    deviceSample: JSON.stringify(r.deviceSample || []).replace(/"/g, '"'),
                    slaveSample: JSON.stringify(r.slaveSample || []).replace(/"/g, '"')
                });
            }
            const ts = new Date().toISOString().slice(0,19).replace(/[:T]/g,'-');
            const reportFileCsv = path.join(EXPORT_DIR, `icy4850_addresssearch_${addressDecimal}_${ts}.csv`);
            writeCsvReport(reportFileCsv, rows);
            writeLog(`WROTE ADDRESS SEARCH REPORT: ${reportFileCsv}`);
            console.log(chalk.green(`Wrote CSV report: ${reportFileCsv}`));
        }
    } catch (e) {
        writeLog(`ERROR WRITING ADDRESS REPORT: ${e.message || e}`,'ERROR');
    }

    return results;
}

function perform_search(answer) {
    try {
        const regex = new RegExp(answer, 'i'); // Case insensitive
        const matches = allSchemas.filter(schema => regex.test(schema));

        console.log(chalk.green(`Gevonden resultaten (${matches.length}):`));
        matches.forEach(match => {
            // Highlight the match
            const matchResult = match.match(regex);
            if (matchResult) {
                const highlighted = match.replace(regex, (m) => chalk.bgYellow.black(m));
                console.log(`- ${highlighted}`);
            } else {
                console.log(`- ${match}`);
            }
        });
    } catch (e) {
        console.log(chalk.red("Ongeldige Regex: " + e.message));
    }
}

async function search_organizations() {
    await fillSchemas();
    console.log(chalk.cyan("************ ZOEK ORGANISATIE ************"));
    const answer = await inquirer.prompt([
        { type: 'input', name: 'term', message: 'Zoekterm (regex ondersteund):' }
    ]);
    perform_search(answer.term);
    console.log(chalk.cyan("************ KLAAR ************"));
    process.exit(0);
}

async function execute() {
    console.log(chalk.bold.blue("************ STARTUP 4850CM DB TOOLS ************"))

    if (!process.env.DB_URL1 || !process.env.DB_URL2 || !process.env.DB_URL_PORT || !process.env.DB_USERNAME || !process.env.DB_PASSWORD) {
        console.error(chalk.red("************ FOUT: GEEN DATABASE GEGEVENS AANWEZIG IN .ENV BESTAND ************"))
        console.log(chalk.red("************ BEËINDIGD ************"))
        process.exit(1);
    }

    // WORKER MODE: require an action arg to avoid accidental worker-mode when only a DB arg is passed
    const hasActionArg = (typeof process.argv[3] !== 'undefined' && String(process.argv[3]).trim() !== '');
    if (process.argv.length > 2 && hasActionArg) {
        const dbChoice = (process.argv[2] || '').toUpperCase();
        const actionChoice = (process.argv[3] || '').toUpperCase();
        const extraArg = process.argv[4];
        const extraArg2 = process.argv[5];

        console.log(chalk.magenta(`Worker Mode: DB=${dbChoice}, Action=${actionChoice}`));

        // select DB URL based on choice
        if (dbChoice === "A") {
            dbUrl = process.env.DB_URL1;
        } else if (dbChoice === "B") {
            dbUrl = process.env.DB_URL2;
        } else {
            console.error(chalk.red(`Onbekende database keuze: ${dbChoice}`));
            process.exit(1);
        }

        // handle actionChoice independently
        if (actionChoice === 'A') {
            const dryRunValue = String(extraArg || extraArg2 || '').toLowerCase();
            const dryRunFlag = ['true', '1', 'yes', 'y', 'dryrun', '--dry-run', '-n'].includes(dryRunValue);
            await execute_set_timedtask_allschemes(dryRunFlag);
        } else if (actionChoice === 'B') {
            await execute_add_settings();
        } else if (actionChoice === 'C') {
            // schema may be provided as extraArg, else prompt
            const schemaArg = extraArg;
            let schema = schemaArg;
            if (!schema) {
                try {
                    schema = await chooseSchemaInteractive();
                } catch (e) {
                    console.error(chalk.red('Kon geen organisatie kiezen: ' + e.message));
                    process.exit(1);
                }
            }
            const dryRunValue = String(extraArg2 || '').toLowerCase();
            const dryRunFlag = ['true', '1', 'yes', 'y', 'dryrun'].includes(dryRunValue);
            await execute_change_schakelsettings_4850cm(schema, dryRunFlag);
        } else if (actionChoice === 'D') {
            await execute_enabled_check();
        } else if (actionChoice === 'E') {
            await get_status_per_park();
        } else if (actionChoice === 'F') {
            console.log(chalk.bold.blue("************ UITDRAAI SCHAKELTIJDEN ************"));
            await uitdraai_schakeltijden_4850();

        } else if (actionChoice === 'G') {
            const searchTerm = (extraArg || '').trim();
            let finalTerm = searchTerm;
            if (!finalTerm) {
                const searchAnswer = await inquirer.prompt([{ type: 'input', name: 'term', message: 'Zoekterm (regex ondersteund):' }]);
                finalTerm = (searchAnswer.term || '').trim();
            }
            if (!finalTerm) { console.log(chalk.red('Geen zoekterm opgegeven.')); process.exit(0); }
            await fillSchemas();
            perform_search(finalTerm);
            console.log(chalk.cyan("************ KLAAR ************"));
            process.exit(0);
        } else if (actionChoice === 'H') {
            // Allow worker-mode to request interactive prompts by passing an interactive flag as extraArg
            const allowInteractiveVals = ['-i', '--interactive', 'interactive', 'true', '1'];
            const interactiveFlag = allowInteractiveVals.includes(String(extraArg).toLowerCase()) || allowInteractiveVals.includes(String(extraArg2).toLowerCase());
            console.log(chalk.magenta(`Worker H: interactive=${interactiveFlag}`));
            await execute_check_and_offer_convert_schakeltijden_all(interactiveFlag);
        } else if (actionChoice === 'I') {
            // Find address across all schemas (worker-mode)
            const allowInteractiveVals = ['-i', '--interactive', 'interactive', 'true', '1'];
            const interactiveFlag = allowInteractiveVals.includes(String(extraArg).toLowerCase()) || allowInteractiveVals.includes(String(extraArg2).toLowerCase());
            let addressArg = extraArg || extraArg2;
            if (!addressArg && interactiveFlag) {
                const answer = await inquirer.prompt([{ type: 'input', name: 'addr', message: 'Adres (decimaal):' }]);
                addressArg = answer.addr;
            }
            let addressDecimal = addressArg;
            if (!addressDecimal) {
                console.error(chalk.red('No address provided for action I.')); process.exit(1);
            }
            // ensure numeric
            addressDecimal = parseInt(addressDecimal, 10);
            if (Number.isNaN(addressDecimal)) { console.error(chalk.red('Invalid address value')); process.exit(1); }
            const res = await findAddressInAllSchemas(addressDecimal);
            console.log(chalk.green(`Search complete. Found ${res.length} locations.`));
            if (res && res.length > 0) {
                console.log(chalk.cyan('Summary:'));
                for (const r of res) {
                    console.log(chalk.cyan(`- ${r.schema}: ${r.count} row(s) matchedType=${r.matchedType}`));
                }
                try {
                    const files = fs.readdirSync(EXPORT_DIR).filter(f => f.startsWith(`icy4850_addresssearch_${addressDecimal}_`)).map(f=>({name:f, m: fs.statSync(path.join(EXPORT_DIR,f)).mtime})).sort((a,b)=>b.m - a.m);
                    if (files && files.length) console.log(chalk.green(`Latest report: ${path.join(EXPORT_DIR, files[0].name)}`));
                } catch (e) {
                    writeLog(`ERROR listing report files: ${e.message || e}`, 'WARN');
                }
            }
            return;
        } else {
            console.error(chalk.red(`Onbekende actie: ${actionChoice}`));
            process.exit(1);
        }
        return;
    }

    // If a DB arg was supplied but no action arg, treat it as a preselected DB for interactive mode
    let preselectedDb = null;
    if (process.argv.length > 2 && !hasActionArg) {
        const dbArg = (process.argv[2] || '').toUpperCase();
        if (dbArg === 'A' || dbArg === 'B') preselectedDb = dbArg;
    }


    // INTERACTIVE MODE WITH INQUIRER (loopable menus, Q = terug, X = afsluiten)
    try {
        while (true) {
            let dbChoice;
            if (preselectedDb) {
                dbChoice = preselectedDb;
                preselectedDb = null; // use only once
            } else {
                printMenuHeader('Selecteer database:');
                const dbChoices = buildLetteredChoices([
                    { letter: 'A', name: 'MySQL Database (icyccdb.icy.nl)', value: 'A' },
                    { letter: 'B', name: 'MariaDB Database (icyccdb02.icy.nl)', value: 'B' },
                    { letter: 'C', name: 'README.MD weergeven', value: 'C' },
                    { letter: 'X', name: 'Afsluiten', value: 'X' }
                ]);

                const dbAnswer = await inquirer.prompt([
                    { type: 'list', name: 'database', message: '', choices: dbChoices, pageSize: INQUIRER_PAGE_SIZE }
                ]);

                if (dbAnswer.database === 'X') {
                    console.log(chalk.yellow('Afsluiten.'));
                    process.exit(0);
                }

                if (dbAnswer.database === 'C') {
                    try {
                        const readmeContent = fs.readFileSync('README.md', 'utf8');
                        console.log(chalk.white(readmeContent));
                    } catch (e) {
                        console.error(chalk.red("Kon README.md niet lezen: " + e.message));
                    }
                    continue; // terug naar database selectie
                }

                dbChoice = dbAnswer.database;
            }

            if (dbChoice === 'A') {
                dbUrl = process.env.DB_URL1;
            } else if (dbChoice === 'B') {
                dbUrl = process.env.DB_URL2;
            }

            // inner action loop
            let backToDb = false;
            while (!backToDb) {
                renderMainHeader(dbChoice);
                printMenuHeader('Wat gaan we doen?');
                const actionChoices = buildLetteredChoices([
                    { letter: 'A', name: '(!) Timedtask toevoegen alle organisaties', value: 'A' },
                    { letter: 'B', name: '(!) Settings toevoegen alle organisaties', value: 'B' },
                    { letter: 'C', name: '(!) Modules omzetten naar 60 seconden schakeltijd (sendlist) voor 1 organisatie', value: 'C' },
                    { letter: 'D', name: 'Check de timedtask van setting ICY4850HARDWARECHECK per organisatie', value: 'D' },
                    { letter: 'E', name: 'Rapport & huidige status ICY4850HARDWAREISSUE per organisatie', value: 'E' },
                    { letter: 'F', name: 'Rapport & huidige status (min & max) ICY4850CM per organisatie', value: 'F' },
                    { letter: 'G', name: 'Zoek organisatie (regex)', value: 'G' },
                    { letter: 'H', name: 'Check & wijzig schakeltijden (alle organisaties)', value: 'H' },
                    { letter: 'I', name: 'Zoek adres in alle organisaties (device & slavedevice)', value: 'I' },
                    { letter: 'Q', name: 'Terug naar Database Selectie', value: 'Q' }
                ]);

                const actionAnswer = await inquirer.prompt([
                    { type: 'list', name: 'action', message: '', choices: actionChoices, pageSize: INQUIRER_PAGE_SIZE }
                ]);

                const action = actionAnswer.action;
                if (action === 'Q') { backToDb = true; continue; }

                if (action === 'A') {
                    console.log(chalk.bold.blue("************ TIMEDTASK TOEVOEGEN ************"));
                    console.log(chalk.yellow("=-=-= CONTROLEER DE TIMEDTASK IN DE CODE HARDCODED OF DEZE NU NOG VAN TOEPASSING ZIJN =-=-="));
                    const confirm = await inquirer.prompt([{ type: 'confirm', name: 'ok', message: 'Doorgaan?', default: false }]);
                    if (confirm.ok) await execute_set_timedtask_allschemes();
                    else continue;

                } else if (action === 'B') {
                    console.log(chalk.bold.blue("************ SETTINGS TOEVOEGEN ************"));
                    console.log(chalk.yellow("=-=-= CONTROLEER DE SETTINGS IN DE CODE HARDCODED OF DEZE NU NOG VAN TOEPASSING ZIJN =-=-="));
                    const confirm = await inquirer.prompt([{ type: 'confirm', name: 'ok', message: 'Doorgaan?', default: false }]);
                    if (confirm.ok) await execute_add_settings();
                    else continue;

                } else if (action === 'C') {
                    console.log(chalk.bold.blue("************ SCHAKELTIJD AANPASSEN MODULES ************"));
                    console.log("Je gaat nu de modules aanpassen naar 60 seconden schakeltijd via de sendlist van een organisatie.");

                    let schema;
                    try {
                        schema = await chooseSchemaInteractive();
                    } catch (e) {
                        console.error(chalk.red('Kon geen organisatie kiezen: ' + e.message));
                        continue;
                    }

                    const dryRunAnswer = await inquirer.prompt([
                        {
                            type: 'list',
                            name: 'mode',
                            message: 'Kies uitvoermodus:',
                            choices: [
                                { name: 'Dry Run (Veilig - Alleen simuleren)', value: true },
                                { name: 'Live (LET OP: Wijzigingen worden doorgevoerd!)', value: false }
                            ],
                            pageSize: INQUIRER_PAGE_SIZE
                        }
                    ]);

                    console.log(`Je gaat nu de sendlist vullen voor organisatie: ${schema}.`);
                    if(dryRunAnswer.mode) console.log(chalk.magenta("MODUS: DRY RUN (Geen wijzigingen)"));
                    else console.log(chalk.red("MODUS: LIVE (Wijzigingen worden opgeslagen!)"));

                    const confirm = await inquirer.prompt([{ type: 'confirm', name: 'ok', message: 'Weet je zeker dat je wilt doorgaan?', default: false }]);

                    if (confirm.ok) {
                        await execute_change_schakelsettings_4850cm(schema, dryRunAnswer.mode);
                    } else {
                        continue;
                    }

                } else if (action === 'D') {
                    console.log(chalk.bold.blue("************ CHECK ENABLED TIMEDTASK STATUS ************"));
                    await execute_enabled_check();

                } else if (action === 'E') {
                    console.log(chalk.bold.blue("************ CHECK STATUS PER PARK ************"));
                    await get_status_per_park();

                } else if (action === 'F') {
                    console.log(chalk.bold.blue("************ UITDRAAI SCHAKELTIJDEN ************"));
                    await uitdraai_schakeltijden_4850();

                } else if (action === 'G') {
                    await fillSchemas();
                    console.log(chalk.cyan("************ ZOEK ORGANISATIE ************"));
                    const searchAnswer = await inquirer.prompt([{ type: 'input', name: 'term', message: 'Zoekterm (regex ondersteund):' }]);
                    if (!searchAnswer.term) { console.log(chalk.red('Geen zoekterm opgegeven.')); continue; }
                    perform_search(searchAnswer.term);
                    console.log(chalk.cyan("************ KLAAR ************"));
                    // continue to action menu
                } else if (action === 'I') {
                    // interactive: ask for decimal address and run search
                    const addrAnswer = await inquirer.prompt([{ type: 'input', name: 'addr', message: 'Adres (decimaal):' }]);
                    if (!addrAnswer.addr) { console.log(chalk.red('Geen adres opgegeven.')); continue; }
                    const addrDec = parseInt(addrAnswer.addr, 10);
                    if (Number.isNaN(addrDec)) { console.log(chalk.red('Ongeldige decimale waarde')); continue; }
                    await fillSchemas();
                    await findAddressInAllSchemas(addrDec);
                    console.log(chalk.cyan("************ KLAAR ************"));
                    continue;
                } else if (action === 'H') {
                    await execute_check_and_offer_convert_schakeltijden_all(true);
                }
            }
        }

    } catch (error) {
        console.error(chalk.red("Er is een fout opgetreden:"), error);
        process.exit(1);
    }
}

execute()
    .then(async () => {
        await closePool();
    })
    .catch(async (error) => {
        try {
            console.error(chalk.red("Onverwachte fout:"), error);
        } catch (e) {
            console.error(error);
        }
        try { await closePool(); } catch (e) {}
        process.exit(1);
    });