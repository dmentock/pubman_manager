# pubman_manager

Tool to automate Publication- and Talk imports to PuRe based on excel spreadsheets.

**Usage**:

- Add PuRe username and password to `env` file
- Add Scopus API key to `env` file (https://dev.elsevier.com/)
- Add Scopus Affiliation id to `env` file (default "60026606" is for MPI Eisenforschung/Sustainable Materials)
- Rename `env` to `.env`
- Before using the `import_*` scripts, run the `pubman_generate_author_and_org_info.ipynb` script once to fetch the current data from the PuRe Database