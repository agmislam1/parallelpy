from modules import codegen, extraction, models


def main():
    #filepath = "../examples/test_reducer.py"
    
    #filepath = "../examples/sum.py"
    #filepath = "../examples/test_reducer.py"
    #filepath = "../examples/reducer_main.py"

    #filepath = "../examples/mapperonly.py"
    filepath = "../examples/mapperonly_1.py"

    #filepath = "../examples/min_a.py"
    #filepath = "../examples/max_a.py"
    #filepath = "../examples/count.py"
    
    program_information = models.ProgramInformation()
    extraction.program_analysis(program_information, filepath)
    #codeGen.codeGen(codeGen.mapper_reducer_generation(program_information),filepath)
    for temp in program_information.all_functions:

        print(temp.name)


if __name__ == "__main__":
    main()
