"""Interactive DB menu for enumerating and managing bridges.

Features:
- List databases on server
- Enumerate bridges per database or all databases
- Add a new bridge (insert into `inbridge`)
- Remove a bridge (safely set referencing devices' inbridgeid to NULL then delete)
- Change bridge association (move devices from one inbridgeid to another)
- Update device MAC/address fields (`devid` or `address`)

Safety: operations prompt for explicit confirmation and use transactions.
"""
from __future__ import annotations
import os
import time
import getpass
try:
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover - helpful fallback when venv missing dependency
    def load_dotenv(*_args, **_kwargs):
        print("Warning: python-dotenv not installed in this environment. Skipping .env load.")
import mysql.connector
from typing import List, Dict, Optional
import shutil
import sys
import subprocess
import tempfile
from pathlib import Path

# Cross-platform single-key reader
if os.name == 'nt':
    import msvcrt
    def _getch():
        return msvcrt.getwch()
else:
    import tty, termios
    def _getch():
        fd = sys.stdin.fileno()
        old = termios.tcgetattr(fd)
        try:
            tty.setraw(fd)
            ch = sys.stdin.read(1)
            return ch
        finally:
            termios.tcsetattr(fd, termios.TCSADRAIN, old)

# last printable character read by _get_key when it returns 'OTHER'
_LAST_CHAR = None


def _get_key():

    """Return one of: 'UP','DOWN','ENTER','ESC','OTHER'"""
    global _LAST_CHAR
    _LAST_CHAR = None
    c = _getch()
    # Windows arrow keys start with '\x00' or '\xe0' then code
    if c in ('\x00', '\xe0') and os.name == 'nt':
        c2 = _getch()
        if c2 == 'H' or c2 == '\x48':
            return 'UP'
        if c2 == 'P' or c2 == '\x50':
            return 'DOWN'
        # PageUp and PageDown on Windows: 'I' (73) and 'Q' (81)
        if c2 == 'I':
            return 'PAGEUP'
        if c2 == 'Q':
            return 'PAGEDOWN'
        return 'OTHER'
    # Unix: arrows are '\x1b[A' '\x1b[B'
    if c == '\x1b':
        # try to read rest non-blocking
        c2 = _getch()
        if c2 == '[':
            # Could be arrow (A/B) or page (5~/6~) sequences
            c3 = _getch()
            if c3 == 'A':
                return 'UP'
            if c3 == 'B':
                return 'DOWN'
            if c3 == '5':
                # consume the trailing '~' if present
                try:
                    c4 = _getch()
                    if c4 == '~':
                        return 'PAGEUP'
                except Exception:
                    return 'PAGEUP'
            if c3 == '6':
                try:
                    c4 = _getch()
                    if c4 == '~':
                        return 'PAGEDOWN'
                except Exception:
                    return 'PAGEDOWN'
        return 'ESC'
    if c == '\r' or c == '\n':
        _LAST_CHAR = None
        return 'ENTER'
    if c == '\x03':
        raise KeyboardInterrupt
    # For printable/non-special keys, expose the actual char via _LAST_CHAR
    _LAST_CHAR = c
    return 'OTHER'


def show_menu(title: str, options: List[str]) -> int:
    """Interactive selector. Returns selected index."""
    selection = 0
    scroll_offset = 0
    try:
        term_size = shutil.get_terminal_size()
        window_height = term_size.lines
        window_width = term_size.columns
    except Exception:
        window_height = 24
        window_width = 80
    list_height = max(5, window_height - 6)

    def clear():
        if os.name == 'nt':
            os.system('cls')
        else:
            os.system('clear')

    # Hide cursor while the interactive menu runs (best-effort)
    try:
        print('\033[?25l', end='')
    except Exception:
        pass

    try:
        while True:
            if selection < scroll_offset:
                scroll_offset = selection
            elif selection >= scroll_offset + list_height:
                scroll_offset = selection - list_height + 1

            clear()
            # Header styling aligned with toolkit.ps1: left-justified cyan title, dim subtitle, gray separator
            header_icon = ICONS.get('db', '') + ' ' if 'ICONS' in globals() else ''
            w = max(10, window_width - 1)
            title_line = f"{header_icon}{title}".ljust(w)
            subtitle = "Gebruik pijltjes om te navigeren, Enter om te selecteren.".ljust(w)
            print("\033[36m" + title_line + "\033[0m")
            print("\033[37m" + subtitle + "\033[0m")
            print("\033[37m" + ("-" * w) + "\033[0m")

            for i in range(list_height):
                idx = scroll_offset + i
                if idx < len(options):
                    prefix = "   "
                    color = '\033[37m'  # white
                    if idx == selection:
                        prefix = '-> '
                        color = '\033[32m'  # green
                    text = f"{prefix}{options[idx]}"
                    if len(text) > window_width:
                        text = text[:window_width-1]
                    print(color + text.ljust(window_width) + '\033[0m')
                else:
                    print('')

            try:
                key = _get_key()
            except KeyboardInterrupt:
                # Treat Ctrl-C as a graceful cancel and return to previous menu
                break
            if key == 'UP':
                selection -= 1
                if selection < 0:
                    selection = len(options) - 1
            elif key == 'DOWN':
                selection += 1
                if selection >= len(options):
                    selection = 0
            elif key == 'ENTER':
                return selection
    except KeyboardInterrupt:
        return -1
    finally:
        # Restore cursor visibility
        try:
            print('\033[?25h', end='')
        except Exception:
            pass


def load_workspace_env():
    here = Path(__file__).resolve()
    candidates = [
        here.parent / ".env",
        here.parents[2] / ".env" if len(here.parents) >= 3 else None,
        here.parents[3] / "python" / "DBscript" / ".env" if len(here.parents) >= 4 else None,
        here.parents[3] / "DBscript" / ".env" if len(here.parents) >= 4 else None,
        Path.cwd() / ".env",
    ]

    for env_path in candidates:
        if env_path and env_path.exists():
            load_dotenv(env_path, override=True)
            return env_path

    load_dotenv()
    return None


ENV_PATH = load_workspace_env()

DB_HOST = os.getenv("DB_HOST")
DB_HOST2 = os.getenv("DB_HOST2")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

MAX_CONNECTION_ATTEMPTS = 3
RETRY_SLEEP = 1

# UI icons and small helpers for nicer menus
ICONS = {
    'db': '🗂️',
    'search': '🔎',
    'back': '🔙',
    'add': '➕',
    'remove': '🗑️',
    'change': '🔀',
    'list': '📋',
    'exit': '❌',
    'details': '🔎',
    'update': '🔧',
}

def option_label(option: str) -> str:
    """Return a decorated option label with an icon based on keywords."""
    o = option
    lower = option.lower()
    icon = ''
    if 'back' in lower or '<back>' in lower:
        icon = ICONS['back'] + ' '
    elif 'exit' in lower:
        icon = ICONS['exit'] + ' '
    elif 'add' in lower:
        icon = ICONS['add'] + ' '
    elif 'remove' in lower or 'delete' in lower:
        icon = ICONS['remove'] + ' '
    elif 'change' in lower or 'move' in lower:
        icon = ICONS['change'] + ' '
    elif 'list' in lower or 'bridges' in lower:
        icon = ICONS['list'] + ' '
    elif 'select' in lower or 'database' in lower:
        icon = ICONS['db'] + ' '
    elif 'details' in lower:
        icon = ICONS['details'] + ' '
    elif 'update' in lower:
        icon = ICONS['update'] + ' '
    return f"{icon}{o}"


def launch_toolkit_menu(toolkit_path: Optional[str] = None) -> bool:
    """Attempt to launch the PowerShell `toolkit.ps1` script to show the Toolkit menu.

    Returns True if the command was invoked, False otherwise.
    """
    # Prefer the workspace absolute path if present, then fall back to relative paths
    preferred = r"C:\Users\h.nijdam\OneDrive - I.C.Y. B.V\Scripts\Toolkit\toolkit.ps1"
    if toolkit_path is None:
        if os.path.exists(preferred):
            toolkit_path = preferred
        else:
            toolkit_path = os.path.normpath(os.path.join(os.path.dirname(__file__), '..', 'Toolkit', 'toolkit.ps1'))
            if not os.path.exists(toolkit_path):
                # fallback to another common relative location
                alt = os.path.normpath(os.path.join(os.path.dirname(__file__), '..', 'Scripts', 'Toolkit', 'toolkit.ps1'))
                if os.path.exists(alt):
                    toolkit_path = alt
    if not os.path.exists(toolkit_path):
        print(f"toolkit.ps1 not found at: {toolkit_path}")
        return False

    shell = shutil.which('pwsh') or shutil.which('powershell.exe') or shutil.which('powershell')
    if not shell:
        print("PowerShell not found on PATH; cannot launch toolkit.ps1")
        return False

    try:
        subprocess.call([shell, '-NoProfile', '-ExecutionPolicy', 'Bypass', '-File', toolkit_path])
        return True
    except Exception as e:
        print(f"Failed to launch toolkit: {e}")
        return False

if not DB_USER:
    DB_USER = input("DB user: ")
if not DB_PASSWORD:
    DB_PASSWORD = getpass.getpass("DB password: ")


def create_connection(database: Optional[str] = None, host: Optional[str] = None):
    """Create a MySQL connection.

    If `host` is provided, try only that host. Otherwise try hosts from
    `DB_HOST` and `DB_HOST2` in order.
    """
    # If caller passed a combined 'host/schema' in the database parameter,
    # split it. Always normalise so `database` contains only the schema name.
    if database and '/' in database:
        maybe_host, maybe_db = database.split('/', 1)
        database = maybe_db
        # simple heuristic: if maybe_host contains a dot assume it's a host
        if '.' in maybe_host:
            # prefer explicit `host` argument if provided, otherwise use maybe_host
            if host is None:
                host = maybe_host
        # else: left part is not a host-looking string; ignore it and use maybe_db

    if host:
        hosts = [host]
    else:
        hosts = [h for h in (DB_HOST, DB_HOST2) if h]
    if not hosts:
        hosts = ["localhost"]
    last_err = None
    for h in hosts:
        for attempt in range(MAX_CONNECTION_ATTEMPTS):
            try:
                conn = mysql.connector.connect(
                    host=h,
                    user=DB_USER,
                    password=DB_PASSWORD,
                    database=database,
                    connect_timeout=10,
                )
                if conn.is_connected():
                    return conn
            except mysql.connector.Error as e:
                last_err = e
                print(f"Connection attempt {attempt+1} to {h} failed: {e}")
                time.sleep(RETRY_SLEEP)
    print(f"Unable to connect to any host. Last error: {last_err}")
    return None


def fetch_databases() -> List[str]:
    """Return a list of databases from all configured hosts.

    Each item is returned as "host/database" so the UI can select which host
    to use.
    """
    hosts = [h for h in (DB_HOST, DB_HOST2) if h]
    if not hosts:
        hosts = ["localhost"]
    dbs: List[str] = []
    for h in hosts:
        conn = create_connection(host=h)
        if not conn:
            continue
        try:
            cur = conn.cursor()
            try:
                cur.execute("SHOW DATABASES")
                rows = cur.fetchall()
                for r in rows:
                    name = r[0]
                    dbs.append(f"{h}/{name}")
            finally:
                cur.close()
        finally:
            conn.close()
    return dbs


def list_bridges(database: str, host: Optional[str] = None) -> List[Dict]:
    conn = create_connection(database, host=host)
    if not conn:
        print(f"Cannot connect to database {database}")
        return []
    try:
        cur = conn.cursor()
        cur.execute("SELECT inbridgeid, hostname, bridgetype, comment, swversion, bridgestate FROM inbridge")
        rows = cur.fetchall()
        cols = [d[0] for d in cur.description] if cur.description else []
        return [dict(zip(cols, row)) for row in rows]
    except mysql.connector.Error as e:
        print(f"Error querying inbridge in {database}: {e}")
        return []
    finally:
        cur.close()
        conn.close()

# Add: pretty columned listing (from list_bridges_prompt) keeping selector style
def list_bridges_for_db(database: str, host: Optional[str] = None) -> None:
    """Interactive table view of `inbridge` for a database.

    Use Up/Down to select a row, Enter to open actions (Details/Remove/Change),
    Esc to return to the database menu. The table refreshes after modifications.
    """
    conn = create_connection(database, host=host)
    if not conn:
        print(f"Unable to connect to database {database}")
        return
    try:
        cur = conn.cursor()
        query = (
            "SELECT "
            "inbridgeid AS id, "
            "bridgetype, "
            "hostname AS macaddress, "
            "comment AS location, "
            "bridgestate AS status, "
            "errortext AS error, "
            "swversion AS `sw version`, "
            "polling, "
            "pollfailure, "
            "localip AS `Local IP-address`, "
            "changetimestamp AS `last changed` "
            "FROM inbridge"
        )

        def load_rows():
            cur.execute(query)
            rows = cur.fetchall()
            cols = [d[0] for d in cur.description] if cur.description else []
            return rows, cols

        rows, cols = load_rows()
        if not rows:
            print("(no bridges)")
            return

        def fmt_val(v):
            return "NULL" if v is None else str(v)

        # compute column widths and cap to avoid overflow
        def compute_col_widths(rows):
            max_width = 100
            col_widths = []
            # column-specific minimums and maximums
            min_map = {
                'location': 28,
                'comment': 28,
                'bridgetype': 30,
                'last changed': 19,
                'changetimestamp': 19,
                'sw version': 8,
                'polling': 3,
                'pollfailure': 3,
                'ip-address local': 15,
                'localip': 15,
                'macaddress': 14,
            }
            max_map = {
                'pollfailure': 6,
                'polling': 6,
                'id': 6,
            }

            for i, col in enumerate(cols):
                max_len = len(str(col))
                for r in rows:
                    vlen = len(fmt_val(r[i]))
                    if vlen > max_len:
                        max_len = vlen

                col_name = str(col).lower()
                # debug instrumentation removed: previously wrote max_len to temp logfile
                # apply minimums
                for key, mn in min_map.items():
                    if key in col_name:
                        if max_len < mn:
                            max_len = mn
                        break

                # apply maximum overrides for small numeric cols
                for key, mx in max_map.items():
                    if key in col_name:
                        if max_len > mx:
                            max_len = mx
                        break

                if max_len > max_width:
                    max_len = max_width
                col_widths.append(max_len)
            return col_widths

        col_widths = compute_col_widths(rows)
        # Enforce per-column minima (defensive): if compute_col_widths missed a min,
        # make sure important columns like `bridgetype` get their minimum width.
        enforced = []
        for col in cols:
            cname = str(col).lower()
            if 'location' in cname or 'comment' in cname:
                enforced.append(12)
            elif 'last changed' in cname or 'changetimestamp' in cname:
                enforced.append(19)
            elif 'bridgetype' in cname:
                enforced.append(30)
            elif 'sw version' in cname or 'swversion' in cname:
                enforced.append(8)
            elif 'pollfailure' in cname:
                enforced.append(3)
            elif 'polling' in cname:
                enforced.append(3)
            elif 'ip' in cname or 'localip' in cname or 'ip-address' in cname:
                enforced.append(15)
            elif 'mac' in cname or 'hostname' in cname or 'macaddress' in cname:
                enforced.append(14)
            elif 'id' == cname:
                enforced.append(4)
            else:
                enforced.append(6)
        for i in range(min(len(col_widths), len(enforced))):
            if col_widths[i] < enforced[i]:
                col_widths[i] = enforced[i]

        try:
            term_size = shutil.get_terminal_size()
            window_height = term_size.lines
            window_width = term_size.columns
        except Exception:
            window_height = 24
            window_width = 80

        def adjust_col_widths_to_window(col_widths, window_width, prefix_len=3):
            # Ensure the total printed width (including separators) fits the window.
            # Use per-column minimums and perform gradual reductions on the
            # widest non-last columns until the total fits or no more reductions
            # are possible. Preserve the last column (IP) as much as possible.
            n = len(col_widths)
            if n == 0:
                return col_widths
            sep_len = 3 * (n - 1)  # ' | ' between columns
            available = max(window_width - prefix_len - sep_len, 0)

            # per-column minimums keyed by substrings
            per_min = []
            for col in cols:
                cname = str(col).lower()
                if 'location' in cname or 'comment' in cname:
                    per_min.append(12)
                elif 'last changed' in cname or 'changetimestamp' in cname:
                    per_min.append(19)
                elif 'bridgetype' in cname:
                    per_min.append(30)
                elif 'sw version' in cname or 'swversion' in cname:
                    per_min.append(8)
                elif 'pollfailure' in cname:
                    per_min.append(3)
                elif 'polling' in cname:
                    per_min.append(3)
                elif 'ip' in cname or 'localip' in cname or 'ip-address' in cname:
                    per_min.append(15)
                elif 'mac' in cname or 'hostname' in cname or 'macaddress' in cname:
                    per_min.append(14)
                elif 'id' == cname:
                    per_min.append(4)
                else:
                    per_min.append(6)

            # debug instrumentation removed: previously wrote window/width data to temp logfile

            total = sum(col_widths)
            if total <= available:
                return col_widths

            # Ensure last column at least its minimum
            last_idx = n - 1
            last_min = per_min[last_idx]
            if col_widths[last_idx] < last_min:
                col_widths[last_idx] = last_min

            # Now reduce other columns gradually (widest first) but not below their min
            new_widths = col_widths[:]
            need_reduce = sum(new_widths) - available
            # debug instrumentation removed
            if need_reduce <= 0:
                return new_widths

            # Create list of indices excluding last and prefer to NOT reduce
            # 'location' and 'bridgetype' until necessary
            loc_idx = None
            br_idx = None
            for ii, cname in enumerate(cols):
                lc = str(cname).lower()
                if 'location' in lc or 'comment' in lc:
                    loc_idx = ii
                if 'bridgetype' in lc:
                    br_idx = ii

            idxs_non_last = [i for i in range(n - 1) if i not in (loc_idx, br_idx)]
            # loop until no more reduction possible or need satisfied
            while need_reduce > 0:
                # sort by current width descending (non-location first)
                idxs_non_last.sort(key=lambda i: new_widths[i], reverse=True)
                reduced_this_round = 0
                for i in idxs_non_last:
                    can_reduce = new_widths[i] - per_min[i]
                    if can_reduce <= 0:
                        continue
                    # reduce by 1 char at a time to be fair
                    new_widths[i] -= 1
                    need_reduce -= 1
                    reduced_this_round += 1
                    if need_reduce <= 0:
                        break
                if reduced_this_round == 0:
                    # try reducing the location column (if present and not last)
                    if loc_idx is not None and loc_idx != last_idx:
                        can_reduce_loc = new_widths[loc_idx] - per_min[loc_idx]
                        if can_reduce_loc > 0:
                            reduce_by = min(can_reduce_loc, need_reduce)
                            new_widths[loc_idx] -= reduce_by
                            need_reduce -= reduce_by
                            reduced_this_round += reduce_by
                    if reduced_this_round == 0:
                        # no further reductions possible on non-last columns or location
                        break

            # If still need_reduce, try reducing last column but not below its min
            if need_reduce > 0:
                can_reduce_last = new_widths[last_idx] - per_min[last_idx]
                reduce_by = min(can_reduce_last, need_reduce)
                new_widths[last_idx] -= reduce_by
                need_reduce -= reduce_by
            # debug instrumentation removed
            return new_widths

        # Adjust computed widths to fit the current window
        col_widths = adjust_col_widths_to_window(col_widths, window_width)
        list_height = max(5, window_height - 8)

        selection = 0
        scroll_offset = 0

        def clear():
            if os.name == 'nt':
                os.system('cls')
            else:
                os.system('clear')

        while True:
            # include a trailing Back sentinel so user can select it
            display_rows = list(rows) + [None]
            total = len(display_rows)

            if selection < scroll_offset:
                scroll_offset = selection
            elif selection >= scroll_offset + list_height:
                scroll_offset = selection - list_height + 1

            clear()
            print(f"\033[36mCustomers Bridges - {database}\033[0m")
            # brief help about navigation keys
            print("\033[37mPageUp/PgDn: jump a page. Press 'b' to back.\033[0m")
            # header (include left padding so data columns align with prefixed rows)
            header_prefix = '   '
            # Truncate header text to the column width before padding to avoid overflow
            header_cells = [str(col)[:col_widths[i]].ljust(col_widths[i]) for i, col in enumerate(cols)]
            print(header_prefix + " | ".join(header_cells))
            underline = ["-" * w for w in col_widths]
            print(header_prefix + "-|-".join(underline))

            visible = display_rows[scroll_offset: scroll_offset + list_height]
            for i, row in enumerate(visible):
                idx = scroll_offset + i
                prefix = '   '
                color = '\033[37m'
                if idx == selection:
                    prefix = '-> '
                    color = '\033[32m'

                if row is None:
                    # render Back row
                    text = prefix + '<Back>'
                    print(color + text.ljust(window_width) + '\033[0m')
                    continue

                # Build row cells, with special handling for the `location` column
                loc_idx = None
                for ci, cname in enumerate(cols):
                    lc = str(cname).lower()
                    if 'location' in lc or 'comment' in lc:
                        loc_idx = ci
                    if 'bridgetype' in lc:
                        br_idx = ci
                    # don't break; want to find both indices if present

                # prepare cell strings (truncated where appropriate)
                base_cells = []
                for j, cell in enumerate(row):
                    s = fmt_val(cell)
                    # For bridgetype prefer direct cut (no ellipsis) so it's fully visible when possible
                    if j == br_idx and len(s) > col_widths[j]:
                        s = s[:col_widths[j]]
                    # For most other columns, prefer ellipsis truncation to keep table tidy
                    elif j != loc_idx and len(s) > col_widths[j]:
                        s = s[: col_widths[j] - 3] + '...'
                    base_cells.append(s)

                # If location needs wrapping, create two-line output for this row
                continuation = None
                if loc_idx is not None:
                    loc_text = base_cells[loc_idx]
                    if len(loc_text) > col_widths[loc_idx]:
                        first_part = loc_text[:col_widths[loc_idx]]
                        rest = loc_text[col_widths[loc_idx]: col_widths[loc_idx]*2]
                        if len(rest) > col_widths[loc_idx] - 3:
                            rest = rest[:col_widths[loc_idx]-3] + '...'
                        continuation = rest
                        base_cells[loc_idx] = first_part

                # Now build printable cells (pad to widths) and print in a consistent way
                padded_cells = [base_cells[k].ljust(col_widths[k])[:col_widths[k]] for k in range(len(base_cells))]

                # Build the single-line representation and ensure it matches header widths
                line = prefix + ' | '.join(padded_cells)
                # If it somehow exceeds the window, trim the right side (should be rare
                # because widths were adjusted earlier) while keeping header alignment
                if len(line) > window_width:
                    line = line[:window_width]
                print(color + line.ljust(window_width) + '\033[0m')

                # If we have a continuation for the location, print a secondary line
                if continuation is not None:
                    blanks = []
                    for k in range(len(padded_cells)):
                        if k == loc_idx:
                            blanks.append(continuation.ljust(col_widths[k])[:col_widths[k]])
                        else:
                            blanks.append(' ' * col_widths[k])
                    cont_line = prefix + ' | '.join(blanks)
                    if len(cont_line) > window_width:
                        cont_line = cont_line[:window_width]
                    print(color + cont_line.ljust(window_width) + '\033[0m')

            # If the Back row is not visible, print a dedicated Back line at the bottom
            back_index = len(rows)
            if not (scroll_offset <= back_index < scroll_offset + list_height):
                # render a Back line (respect selection)
                b_prefix = '   '
                b_color = '\033[37m'
                if selection == back_index:
                    b_prefix = '-> '
                    b_color = '\033[32m'
                print(b_color + (b_prefix + '<Back>').ljust(window_width) + '\033[0m')

            key = _get_key()
            if key == 'UP':
                selection = (selection - 1) % total
            elif key == 'DOWN':
                selection = (selection + 1) % total
            elif key == 'PAGEUP':
                # Move up by one page
                selection = max(0, selection - list_height)
                scroll_offset = max(0, scroll_offset - list_height)
                # ensure selection stays within range
                selection = min(selection, total - 1)
                continue
            elif key == 'PAGEDOWN':
                # Move down by one page
                selection = min(total - 1, selection + list_height)
                # advance scroll_offset but don't exceed maximal offset
                max_offset = max(0, total - list_height)
                scroll_offset = min(max_offset, scroll_offset + list_height)
                continue
            elif key == 'ENTER':
                sel = display_rows[selection]
                if sel is None:
                    # Back selected
                    break
                sel_row = sel
                # build a simple actions menu
                actions = ['Details', 'Remove', 'Change association (move devices)', 'Back']
                act = show_menu(f"Acties voor bridge {sel_row[0]}", actions)
                if act == 0:
                    clear()
                    print('Details:')
                    for name, val in zip(cols, sel_row):
                        print(f"{name}: {fmt_val(val)}")
                    input('Press Enter to continue...')
                elif act == 1:
                    confirm = input(f"Verwijder bridge {sel_row[0]}? Type 'yes' to confirm: ")
                    if confirm.lower() == 'yes':
                        ok = remove_bridge(database, int(sel_row[0]), host=host)
                        if ok:
                            print('Removed.')
                            # refresh rows
                            rows, cols = load_rows()
                            col_widths = compute_col_widths(rows)
                            selection = min(selection, max(0, len(rows)-1))
                            time.sleep(0.5)
                        else:
                            print('Remove failed.')
                            time.sleep(1)
                elif act == 2:
                    target = input('New inbridgeid to move devices to: ').strip()
                    try:
                        if change_bridge_association(database, int(sel_row[0]), int(target), host=host):
                            print('Association changed.')
                        else:
                            print('Association change failed.')
                    except ValueError:
                        print('Invalid id')
                    input('Press Enter to continue...')
                else:
                    # Back from actions - return to table
                    continue
            elif key == 'ESC':
                break
            # allow pressing 'b' to go back as a convenience
            elif key == 'OTHER':
                # Use the last printable char exposed by _get_key to avoid blocking
                ch2 = globals().get('_LAST_CHAR', '')
                if ch2 in ('b', 'B'):
                    break
            else:
                # ignore other keys
                continue

    except mysql.connector.Error as e:
        print(f"Query error: {e}")
    finally:
        try:
            cur.close()
        except Exception:
            pass
        try:
            conn.close()
        except Exception:
            pass
def choose_database(databases: List[str]) -> Optional[str]:
    """Interactive chooser with incremental search: type to narrow the list.

    - type characters to filter (case-insensitive substring)
    - Backspace deletes
    - Up/Down move selection
    - Enter selects
    - Esc or Ctrl-C cancels
    Returns the selected database name or None if cancelled.
    """
    if not databases:
        return None

    try:
        term_size = shutil.get_terminal_size()
        window_height = term_size.lines
        window_width = term_size.columns
    except Exception:
        window_height = 24
        window_width = 80
    list_height = max(5, window_height - 7)

    query = ""
    selection = 0
    scroll_offset = 0

    def clear():
        if os.name == 'nt':
            os.system('cls')
        else:
            os.system('clear')

    def filtered_items():
        if not query:
            return databases
        q = query.lower()
        return [d for d in databases if q in d.lower()]

    while True:
        items = filtered_items()
        # Always show a final 'Back' option so user can explicitly go back
        display_items = items + ['<Back>']
        if selection >= len(items):
            # allow selection to land on the Back item as well
            selection = max(0, len(display_items) - 1)

        if selection < scroll_offset:
            scroll_offset = selection
        elif selection >= scroll_offset + list_height:
            scroll_offset = selection - list_height + 1

        clear()
        print(f"\033[36mChoose database{(' - filter: ' + query) if query else ''}\033[0m")
        print("Type to filter, arrows to navigate, Enter to select, Esc to cancel")
        print("-" * min(window_width, 200))

        for i in range(list_height):
            idx = scroll_offset + i
            if idx < len(display_items):
                prefix = '   '
                color = '\033[37m'
                if idx == selection:
                    prefix = '-> '
                    color = '\033[32m'
                text = f"{prefix}{display_items[idx]}"
                if len(text) > window_width:
                    text = text[:window_width-1]
                print(color + text.ljust(window_width) + '\033[0m')
            else:
                print('')

        # read a key directly
        try:
            ch = _getch()
        except KeyboardInterrupt:
            return None

        # Handle arrow sequences and special keys
        if ch in ('\x00', '\xe0') and os.name == 'nt':
            c2 = _getch()
            if c2 in ('H', '\x48'):
                # up
                selection = (selection - 1) % max(1, len(display_items))
            elif c2 in ('P', '\x50'):
                selection = (selection + 1) % max(1, len(display_items))
            continue

        if ch == '\x1b':
            # possible escape or arrow on unix
            # try to read two more chars for arrow sequences
            try:
                c2 = _getch()
                if c2 == '[':
                    c3 = _getch()
                    if c3 == 'A':
                        selection = (selection - 1) % max(1, len(display_items))
                        continue
                    if c3 == 'B':
                        selection = (selection + 1) % max(1, len(display_items))
                        continue
                # if we got here it's a plain ESC -> cancel
                return None
            except Exception:
                return None

        # Enter
        if ch in ('\r', '\n'):
            # if user selected the Back item, return None
            if selection == len(items):
                return None
            if items:
                return items[selection]
            else:
                continue

        # Backspace
        if ch in ('\x7f', '\b', '\x08'):
            if query:
                query = query[:-1]
                selection = 0
            continue

        # Ctrl-C
        if ch == '\x03':
            return None

        # Printable characters: append to query
        if ord(ch) >= 32:
            query += ch
            selection = 0
            continue

        # ignore other keys
        continue


def add_bridge(database: str, host: Optional[str] = None) -> Optional[int]:
    """Prompt minimally (MAC as hostname, locatie as comment) and insert with requested defaults."""
    print(f"Adding new bridge to database: {database}")
    hostname = input("MAC address (hostname) (required): ").strip()
    if not hostname:
        print("Cancelled: hostname (MAC) is required.")
        return None

    # Defaults per user's request
    bridgetype = 'ICY4816'
    pcinterfaceaddr = 21845
    address = 43947
    port = 10002
    inuse = 1
    input_f = 1
    output_f = 1
    comment = input("Locatie (comment, optional): ").strip() or None
    simnumber = None
    numbernoinreceive = 2000
    errortext = None
    bridgestate = None
    swversion = input("SW version (optional): ").strip() or None

    conn = create_connection(database, host=host)
    if not conn:
        print("Connection failed")
        return None
    try:
        cur = conn.cursor()
        cur.execute(
            """INSERT INTO inbridge (
                hostname, bridgetype, pcinterfaceaddr, address, port,
                inuse, input, output, comment, simnumber, numbernoinreceive,
                errortext, bridgestate, changetimestamp, swversion, polling,
                pollfailure, localip, localipts, swversioninmodul
            ) VALUES (
                %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW(),%s,%s,%s,%s,%s,%s
            )""",
            (
                hostname,
                bridgetype,
                pcinterfaceaddr,
                address,
                port,
                inuse,
                input_f,
                output_f,
                comment,
                simnumber,
                numbernoinreceive,
                errortext,
                bridgestate,
                swversion,
                0,  # polling
                0,  # pollfailure
                None,  # localip
                None,  # localipts
                0,  # swversioninmodul
            ),
        )
        conn.commit()
        new_id = cur.lastrowid
        if not new_id:
            try:
                cur.execute("SELECT LAST_INSERT_ID()")
                row = cur.fetchone()
                if row:
                    new_id = int(row[0])
            except Exception:
                new_id = None
        if new_id is None:
            print("Inserted but could not determine new id.")
        else:
            print(f"Inserted new inbridge with id: {new_id}")
            try:
                ans = input("Do you want to restart the service via Toolkit? (Y/N): ").strip().lower()
            except KeyboardInterrupt:
                ans = 'n'
            if ans == 'y':
                launch_toolkit_menu()
                sys.exit(0)
        return new_id
    except mysql.connector.Error as e:
        conn.rollback()
        print(f"Failed to insert bridge: {e}")
        return None
    finally:
        try:
            cur.close()
        except Exception:
            pass
        try:
            conn.close()
        except Exception:
            pass


def remove_bridge(database: str, inbridgeid: int, host: Optional[str] = None) -> bool:
    """Safely remove an inbridge: set referencing devices' inbridgeid to NULL then delete."""
    conn = create_connection(database, host=host)
    if not conn:
        print("Connection failed")
        return False
    try:
        cur = conn.cursor()
        try:
            cur.execute("START TRANSACTION")
            cur.execute("UPDATE device SET inbridgeid = NULL WHERE inbridgeid = %s", (inbridgeid,))
            cur.execute("DELETE FROM inbridge WHERE inbridgeid = %s", (inbridgeid,))
            conn.commit()
            return True
        except mysql.connector.Error as e:
            conn.rollback()
            print(f"Failed to remove bridge: {e}")
            return False
    finally:
        cur.close()
        conn.close()


def change_bridge_association(database: str, old_id: int, new_id: int, host: Optional[str] = None) -> bool:
    """Move devices from old inbridgeid to new inbridgeid."""
    conn = create_connection(database, host=host)
    if not conn:
        print("Connection failed")
        return False
    try:
        cur = conn.cursor()
        try:
            cur.execute("UPDATE device SET inbridgeid = %s WHERE inbridgeid = %s", (new_id, old_id))
            conn.commit()
            try:
                ans = input("Do you want to restart the service via Toolkit? (Y/N): ").strip().lower()
            except KeyboardInterrupt:
                ans = 'n'
            if ans == 'y':
                launch_toolkit_menu()
                sys.exit(0)
            return True
        except mysql.connector.Error as e:
            conn.rollback()
            print(f"Failed to change association: {e}")
            return False
    finally:
        cur.close()
        conn.close()


def update_device_field(database: str, host: Optional[str] = None) -> None:
    """Prompt to update a device's `devid` or `address` field."""
    db = database
    devid = input("Device id (primary key) to update: ").strip()
    if not devid:
        print("Cancelled")
        return
    field = input("Field to update (devid/address): ").strip()
    if field not in ("devid", "address"):
        print("Unsupported field")
        return
    value = input(f"New value for {field}: ").strip()
    conn = create_connection(db, host=host)
    if not conn:
        print("Connection failed")
        return
    try:
        cur = conn.cursor()
        try:
            cur.execute(f"UPDATE device SET {field} = %s WHERE devid = %s", (value, devid))
            conn.commit()
            print("Updated.")
            try:
                ans = input("Do you want to restart the service via Toolkit? (Y/N): ").strip().lower()
            except KeyboardInterrupt:
                ans = 'n'
            if ans == 'y':
                launch_toolkit_menu()
                sys.exit(0)
        except mysql.connector.Error as e:
            conn.rollback()
            print(f"Failed to update device: {e}")
    finally:
        cur.close()
        conn.close()


def update_device_mac(database: str, host: Optional[str] = None) -> None:
    """Prompt to update a device's MAC address (stored in `address`).

    This is a focused flow used by the menu entry "Update device MAC-address"
    so the user isn't asked to choose a field — we directly update `address`.
    """
    db = database
    devid = input("Device id (primary key) to update: ").strip()
    if not devid:
        print("Cancelled")
        return
    value = input("New MAC address (address): ").strip()
    if not value:
        print("Cancelled: no value provided")
        return
    conn = create_connection(db, host=host)
    if not conn:
        print("Connection failed")
        return
    try:
        cur = conn.cursor()
        try:
            cur.execute("UPDATE device SET address = %s WHERE devid = %s", (value, devid))
            conn.commit()
            print("Updated MAC/address.")
            try:
                ans = input("Do you want to restart the service via Toolkit? (Y/N): ").strip().lower()
            except KeyboardInterrupt:
                ans = 'n'
            if ans == 'y':
                launch_toolkit_menu()
                sys.exit(0)
        except mysql.connector.Error as e:
            conn.rollback()
            print(f"Failed to update device MAC: {e}")
    finally:
        try:
            cur.close()
        except Exception:
            pass
        try:
            conn.close()
        except Exception:
            pass


def update_inbridge_mac(database: str, host: Optional[str] = None) -> None:
    """Update the MAC address stored in `inbridge.hostname` for a bridge.

    Prompts for the `inbridgeid` (primary key) and the new MAC value. Provides
    clear feedback about whether the update affected any rows.
    """
    db = database
    bridge_id = input("Inbridge id (primary key) to update: ").strip()
    if not bridge_id:
        print("Cancelled")
        return
    new_mac = input("New MAC address (hostname): ").strip()
    if not new_mac:
        print("Cancelled: no value provided")
        return

    conn = create_connection(db, host=host)
    if not conn:
        print("Connection failed")
        return
    try:
        cur = conn.cursor()
        try:
            cur.execute("UPDATE inbridge SET hostname = %s WHERE inbridgeid = %s", (new_mac, bridge_id))
            conn.commit()
            if cur.rowcount == 0:
                print("No bridge updated: check the inbridge id you provided.")
            else:
                print(f"Updated bridge {bridge_id} hostname -> {new_mac} (rows affected: {cur.rowcount})")
                try:
                    ans = input("Do you want to restart the service via Toolkit? (Y/N): ").strip().lower()
                except KeyboardInterrupt:
                    ans = 'n'
                if ans == 'y':
                    launch_toolkit_menu()
                    sys.exit(0)
        except mysql.connector.Error as e:
            conn.rollback()
            print(f"Failed to update inbridge hostname: {e}")
    finally:
        try:
            cur.close()
        except Exception:
            pass
        try:
            conn.close()
        except Exception:
            pass


def manage_database_menu(database: str):
    """Database-specific menu: show table, allow selecting a bridge and perform actions.

    `database` may be in the form "host/schema" (as returned by `fetch_databases`).
    """
    # parse host/schema
    host = None
    schema = database
    if isinstance(database, str) and '/' in database:
        host, schema = database.split('/', 1)

    try:
        while True:
            options = ["List bridges", "Add bridge", "Change bridge association (old->new)", "Update bridge MAC-address", "Back"]
            choice = show_menu(f"Bridges - {schema}", options)
            if choice == 0:
                list_bridges_for_db(schema, host=host)
                try:
                    input("Press Enter to continue...")
                except KeyboardInterrupt:
                    return
                continue
            elif choice == 1:
                nid = add_bridge(schema, host=host)
                if nid is not None:
                    print(f"Created inbridge id {nid}")
                else:
                    print("Bridge creation returned no id.")
                try:
                    input("Press Enter to continue...")
                except KeyboardInterrupt:
                    return
            elif choice == 2:
                try:
                    old = input("Old inbridgeid: ").strip()
                    new = input("New inbridgeid: ").strip()
                except KeyboardInterrupt:
                    return
                try:
                    ok = change_bridge_association(schema, int(old), int(new), host=host)
                except ValueError:
                    print("Invalid ids")
                    ok = False
                if ok:
                    print("Association changed.")
                else:
                    print("Association change failed.")
                try:
                    input("Press Enter to continue...")
                except KeyboardInterrupt:
                    return
            elif choice == 3:
                try:
                    update_inbridge_mac(schema, host=host)
                except KeyboardInterrupt:
                    return
                try:
                    input("Press Enter to continue...")
                except KeyboardInterrupt:
                    return
            else:
                break
    except KeyboardInterrupt:
        return


def main_menu():
    databases = fetch_databases()
    if not databases:
        print("No databases available.")
        return
    options = [
        "Select a database and manage bridges",
        "Bridge health scan (alle bridges, export ca. 10 min)",
        "Poll fails scan (>15% fails, export)",
        "Historische log backup (laatste 14 dagen)",
        "Exit"
    ]
    # Detect venv python.exe
    venv_python = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'virt-dahs', 'Scripts', 'python.exe'))
    if not os.path.exists(venv_python):
        venv_python = sys.executable
    try:
        while True:
            choice = show_menu("=== DB Menu ===", options)
            if choice == 0:
                db = choose_database(databases)
                if not db:
                    continue
                manage_database_menu(db)
            elif choice == 1:
                # Bridge health scan
                print("Bridge health scan wordt gestart...")
                try:
                    subprocess.call([venv_python, os.path.join(os.path.dirname(__file__), "list_bridges_prompt.py"), "--action", "all", "--export", "./bridge_scan_menu_output", "--gap-minutes", "20", "--window-days", "4", "--restart-window-threshold", "20"])
                except Exception as e:
                    print(f"Fout bij uitvoeren bridge health scan: {e}")
                try:
                    input("Druk op Enter om terug te keren naar het menu...")
                except KeyboardInterrupt:
                    return
            elif choice == 2:
                # Poll fails scan
                print("Poll fails scan wordt gestart...")
                try:
                    subprocess.call([venv_python, os.path.join(os.path.dirname(__file__), "list_bridges_prompt.py"), "--action", "pollall", "--export", "./pollfail_menu_output", "--poll-threshold", "15"])
                except Exception as e:
                    print(f"Fout bij uitvoeren poll fails scan: {e}")
                try:
                    input("Druk op Enter om terug te keren naar het menu...")
                except KeyboardInterrupt:
                    return
            elif choice == 3:
                print("Historische log backup over de laatste 14 dagen wordt gestart...")
                try:
                    subprocess.call([venv_python, os.path.join(os.path.dirname(__file__), "backup_recent_logs.py"), "--days", "14"])
                except Exception as e:
                    print(f"Fout bij uitvoeren van de log backup: {e}")
                try:
                    input("Druk op Enter om terug te keren naar het menu...")
                except KeyboardInterrupt:
                    return
            elif choice == 4 or choice is None or choice < 0:
                break
            else:
                print("Invalid choice")
                try:
                    input("Press Enter to continue...")
                except KeyboardInterrupt:
                    return
    except KeyboardInterrupt:
        # Ctrl-C at top level -> exit
        print()
        return

def run_manage_bridges_only():
    databases = fetch_databases()
    if not databases:
        print("No databases available.")
        return

    db = choose_database(databases)
    if not db:
        return

    manage_database_menu(db)


if __name__ == '__main__':
    if '--manage-bridges' in sys.argv:
        run_manage_bridges_only()
    else:
        main_menu()
