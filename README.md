# ParallelPy

ParallelPy is a tool designed for running PyDaskShift directly from a GitHub repository. PyDaskShift is the initial step in converting sequential Python programs into distributed parallel programs using the Dask Bag library.

## Getting Started

Before you can utilize PyDaskShift, ensure Git is installed on your computer. If not, you can download it from the [official website](https://git-scm.com/).

Once Git is installed, follow these steps:

1. Open a terminal or command prompt.
2. Navigate to the directory where you want to clone the repository.
3. Clone the repository using the command: git clone https://github.com/agmislam1/parallelpy.git
4. After cloning the repository, navigate into it using: cd ParallelPy/parallelpy

5. 
## Directory Structure

The repsotiory may not contain the some of the following folders:

- **data:** Contains data files utilized by PyDaskShift.
- **examples:** Includes example files suitable for use as input for PyDaskShift.
- **output:** Stores output files generated by PyDaskShift.
- **modules:** Houses the primary source code for PyDaskShift.
- **result:** Contains result files produced by PyDaskShift.


Before running the program, create the folders in the main directory.

## File Structure

Within the `parallelpy` directory, you'll find the following files:

- **__init__.py:** An empty file indicating that this directory should be recognized as a Python package.
- **analysis.py:** Contains code for analyzing data or results.
- **ast_test.py:** Contains code for testing abstract syntax trees (ASTs).
- **main.py:** Serves as the main entry point for the program, executing the primary functionality of the ParallelPy tool.

## Running PyDaskShift

To execute PyDaskShift, follow these steps:

1. Open a terminal or command prompt.
2. Navigate to the `ParallelPy/parallelpy` directory.
3. Use the command: python main.py
4. If you need to change the input file used by PyDaskShift, edit the value of the `filepath` variable in `main.py`. Example files are available in the `/examples` folder.

## Contributing

If you encounter any issues while using ParallelPy or have suggestions for improvements, please feel free to open an issue or submit a pull request on [GitHub](https://github.com/agmislam1/parallelpy).


