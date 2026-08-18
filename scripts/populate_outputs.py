"""Execute python notebook cells and populate output streams into .ipynb files."""
import sys
import os
import io
import contextlib
import glob
from pathlib import Path
import nbformat
import jupytext

ROOT = Path(__file__).resolve().parents[1]
NB_DIR = ROOT / "notebooks"

def run_and_populate(py_path: str, ipynb_path: str):
    print(f"Executing and populating outputs for {os.path.basename(py_path)}...")
    nb = jupytext.read(py_path)
    
    # Execution environment
    glob_dict = {
        "__file__": py_path,
        "__name__": "__main__",
    }
    
    # Ensure sys.path contains notebooks/ and scripts/
    os.chdir(NB_DIR)
    if str(NB_DIR) not in sys.path:
        sys.path.insert(0, str(NB_DIR))
    if str(ROOT / "scripts") not in sys.path:
        sys.path.insert(0, str(ROOT / "scripts"))
        
    for cell in nb.cells:
        if cell.cell_type == "code":
            code = cell.source
            if not code.strip():
                continue
            f = io.StringIO()
            try:
                with contextlib.redirect_stdout(f), contextlib.redirect_stderr(f):
                    exec(code, glob_dict)
                out_text = f.getvalue()
                if out_text:
                    cell.outputs = [
                        nbformat.v4.new_output(
                            output_type="stream",
                            name="stdout",
                            text=out_text
                        )
                    ]
            except Exception as e:
                out_text = f.getvalue() + f"\nError: {e}"
                cell.outputs = [
                    nbformat.v4.new_output(
                        output_type="stream",
                        name="stderr",
                        text=out_text
                    )
                ]
                print(f"  [ERROR in {os.path.basename(py_path)}]: {e}")
                
    nbformat.write(nb, ipynb_path)
    print(f"  ✓ Saved {os.path.basename(ipynb_path)} with outputs.")

def main():
    py_files = sorted(glob.glob(str(ROOT / "notebooks" / "*.py")))
    for py_file in py_files:
        if os.path.basename(py_file).startswith("_"):
            continue
        ipynb_file = py_file.replace(".py", ".ipynb")
        run_and_populate(py_file, ipynb_file)
        
    spark_py_files = sorted(glob.glob(str(ROOT / "notebooks-spark" / "*.py")))
    for py_file in spark_py_files:
        ipynb_file = py_file.replace(".py", ".ipynb")
        nb = jupytext.read(py_file)
        nbformat.write(nb, ipynb_file)

if __name__ == "__main__":
    main()
