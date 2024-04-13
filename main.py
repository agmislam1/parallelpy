from modules import codegen, extraction, models

import sys




def main():
    #filepath = "../examples/test_reducer.py"
    
    filepath = "../examples/sum.py"
    #filepath = "../examples/avg.py"
    filepath = "../examples/test_reducer.py"  # does not work
    #filepath = "../examples/reducer_main.py"
    #filepath = "../examples/reducer_test.py"

    #filepath = "../examples/mapperonly.py"
    #filepath = "../examples/mapperonly_1.py"
    #filepath = "../examples/mapperonly_failed.py"

    #filepath = "../examples/min_a.py"
    #filepath = "../examples/max_a.py"
    #filepath = "../examples/count.py"

    #filepath = "../examples/reducer_example1.py"
    #filepath = "../examples/KNN.py"
    #filepath = "../examples/sql_join.py"
    filepath = "../examples/pie.py"
    #filepath = "../examples/sql_join_2.py"
    #filepath = "../examples/nested_loop_1.py"

    #filepath = "../examples/mapperonly_1.py"
    filepath = "../examples/KNN_v2.py"
    filepath = "../examples/reducer_test.py"
    
    program_information = models.ProgramInformation()
    extraction.program_analysis(program_information, filepath)
    
    # The following line is for synthetic anlaysis....
    codegen.codeGen(codegen.mapper_reducer_generation(program_information),filepath)
    for temp in program_information.all_functions:

        print(temp.name)


if __name__ == "__main__":
    
    main()
