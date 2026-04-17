import json

with open('padelspot copy.ipynb', encoding='utf-8') as f:
    nb = json.load(f)

print('Dashboard-related cells from padelspot copy.ipynb:')
for i, c in enumerate(nb['cells']):
    src = ''.join(c.get('source', []))
    if any(k in src.lower() for k in ['plotly', 'ipywidgets', 'dash', 'carte']):
        ct = c.get('cell_type')
        print(f'[{i}] {ct} - {src[:150].replace(chr(10), " ")}')
