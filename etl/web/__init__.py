"""Web interface (Member A): pick one pipeline, run it, and read the stored report in a browser.

    python -m etl ui                  # then open http://127.0.0.1:8000

It is a thin layer over the same controller the CLI uses (etl.controller.execute_run) and the
same report reader (etl.rdbms.report), so a run started here is identical to `python -m etl run`.
Standard library only: no web framework to install.
"""
