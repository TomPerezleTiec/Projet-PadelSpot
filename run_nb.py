import nbformat
from nbclient import NotebookClient
import sys
import os

with open('/home/jovyan/work/padelspot.ipynb') as f:
    nb = nbformat.read(f, as_version=4)

client = NotebookClient(nb, timeout=600, kernel_name='python3', record_timing=False)

print("Starting execution of padelspot.ipynb")
client.kc = client.start_new_kernel_client()
client.km.start_channels()

for idx, cell in enumerate(nb.cells):
    if cell.cell_type != 'code':
        continue
    
    # Check if cell is an installation cell or just empty
    src = cell.source.strip()
    if not src:
        continue
    
    title = src.split('\n')[0][:50]
    print(f"Executing cell {idx} ({title})...", flush=True)
    try:
        client.execute_cell(cell, idx)
    except Exception as e:
        print(f"\nERROR in cell {idx}:")
        for output in cell.get('outputs', []):
            if output.output_type == 'error':
                print(output.ename, ":", output.evalue)
                for trace in output.traceback:
                    print(trace)
        
        # Save notebook with error state
        with open('/home/jovyan/work/padelspot_failed.ipynb', 'w') as f:
            nbformat.write(nb, f)
            
        sys.exit(1)

client.km.shutdown_kernel()
print("Execution COMPLETED successfully!")
with open('/home/jovyan/work/padelspot_success.ipynb', 'w') as f:
    nbformat.write(nb, f)
