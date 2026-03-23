"""Check JS bracket balance with proper string handling."""
html = open('c:/Users/Dell/Downloads/InteliRisk/static/index.html', encoding='utf-8').read()
idx = html.rfind('<script>')
end = html.find('</script>', idx)
js = html[idx+8:end]

i = 0
stack = []
in_str = None
line = 1
col = 0
errors = []

while i < len(js):
    ch = js[i]
    col += 1

    if ch == '\n':
        line += 1
        col = 0
        i += 1
        continue

    # Handle escape in strings
    if in_str and ch == chr(92):  # backslash
        i += 2
        col += 1
        continue

    # String boundaries
    if in_str:
        if ch == in_str:
            in_str = None
        i += 1
        continue

    if ch in ('"', "'", '`'):
        in_str = ch
        i += 1
        continue

    # Line comments
    if ch == '/' and i+1 < len(js) and js[i+1] == '/':
        nl = js.find('\n', i)
        if nl == -1:
            break
        i = nl
        continue

    # Block comments
    if ch == '/' and i+1 < len(js) and js[i+1] == '*':
        close = js.find('*/', i+2)
        if close == -1:
            errors.append(f'Unclosed block comment at line {line}')
            break
        line += js[i:close+2].count('\n')
        i = close + 2
        continue

    if ch in ('(', '{', '['):
        stack.append((ch, line, col))
    elif ch in (')', '}', ']'):
        expected = {'(': ')', '{': '}', '[': ']'}
        if not stack:
            errors.append(f'Extra closing {ch} at line {line}, col {col}')
        else:
            open_ch, open_line, open_col = stack[-1]
            if expected[open_ch] == ch:
                stack.pop()
            else:
                errors.append(f'Mismatched: opened {open_ch} at line {open_line}, closed with {ch} at line {line}')
                stack.pop()
    i += 1

if stack:
    for open_ch, open_line, open_col in stack:
        errors.append(f'Unclosed {open_ch} opened at JS line {open_line}')

if errors:
    for e in errors[:10]:
        print(e)
else:
    print('No bracket errors found')

html_script_line = html[:idx].count('\n') + 1
print(f'\nJS starts at HTML line {html_script_line}')
