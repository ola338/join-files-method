# Join files method
Implementation of join method which read two csv files, merge them using a specified column and write the result to the output csv file.

## How to run
1. Clone the repository
 `git clone https://github.com/ola338/join-files-method.git`
3. Install required packages:
 `pip install -r requirements.txt`
4. Add your csv files to example_files directory or use existing.
5. Finally you can run a program:
- `python main.py <method> <file_path> <file_path> <column_name> <join_type>`
- e.g. `python main.py join example_files/file1.csv example_files/file2.csv id left`
