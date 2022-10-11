import ast


# Store Program information
class ProgramInformation:
    def __init__(self):
        self.all_functions = []


# Store all function information
class FunctionInformation:

    def __init__(self):
        self.name = []
        self.iteration = []
        self.input_variable = []
        self.return_variable = []  # TODO:  add return type for generation
        self.return_type = ""


# Store all loop information [Right now it is only for loop]
class LoopInformation:

    def __init__(self, initial_line_number: int, final_line_number: int):
        self.loop_variables = ''
        self.allVariables = []
        self.initial_line_number = initial_line_number
        self.final_line_number = final_line_number
        self.operations = []
        self.conditions = []
        self.mainOperations = ""
        self.compareInformation = []

    def add_conditions(self, condition):
        self.conditions.append(condition)

    def add_operations(self, operation):
        self.operations.append(operation)

    def add_variables(self, variable):
        self.allVariables.append(variable)

    def add_comapre_information(self,compare_info):
        self.compareInformation.append(compare_info)


# Operation information stored in (left, op,  right)
class OperationInformation:

    def __init__(self, left, op, right, target):
        self.left = left
        self.op = op
        self.right = right
        self.target = target


# Stores all if/else condition for us
class CompareInformation:

    def __init__(self,left, ops, compare):
        self.left = left
        self.ops = ops
        self.compare = compare


# Stores all replace line information
class LoopReplace:
    def __init__(self, initial_line_no, final_line_number, replace_strings):
        self.initial_line_no = initial_line_no
        self.final_line_number = final_line_number
        self.replace_strings = replace_strings

# Return First and Last Linenumber
def _compute_interval(node):
    min_lineno = node.lineno
    max_lineno = node.lineno
    for node in ast.walk(node):
        if hasattr(node, "lineno"):
            min_lineno = min(min_lineno, node.lineno)
            max_lineno = max(max_lineno, node.lineno)
    return min_lineno, max_lineno


# Extracts all loops from a function node
def extracted_loops(node):
    if isinstance(node, ast.For):
        min_line_no, max_line_no = _compute_interval(node)
        temp_f = LoopInformation(min_line_no, max_line_no)
        has_compare = check_for_compare(node)
        for node in ast.walk(node):
            # print(node) get all variables
            if isinstance(node, ast.Name) and isinstance(node.ctx, ast.Store):
                if not temp_f.allVariables.__contains__(node.id):
                    print(node.id)
                    temp_f.add_variables(node.id)
            # Only for Sum and Count
            if not has_compare:
                if isinstance(node, ast.Assign):
                    print(node)
                    if isinstance(node.value, ast.BinOp):
                        left = variable_check(node.value.left)
                        op = node.value.op
                        right = variable_check(node.value.right)
                        target = node.targets[0].id
                        if target != left:
                            temp = left
                            left = right
                            right = temp
                        print(left, op, right, target)
                        temp_f.add_operations(OperationInformation(left, op, right, target))
                        if right == "1":
                            temp_f.mainOperations = "COUNT"
                        else:
                            temp_f.mainOperations = "ADD"
            else:
                if isinstance(node, ast.If):
                    compare_info = CompareInformation(node.test.left.id, node.test.ops[0], node.test.comparators[0].id )
                    temp_f.add_comapre_information(compare_info)
                    print(compare_info)
                    for ifbody in node.body:
                        if isinstance(ifbody, ast.Assign):
                            target = ifbody.targets[0].id
                            if isinstance(compare_info.ops,ast.Lt):
                                temp_f.mainOperations = "MAX"
                                temp_f.add_operations(OperationInformation(target, compare_info.ops, "", target))
                            else:
                                temp_f.mainOperations = "MIN"
                                temp_f.add_operations(OperationInformation(target, compare_info.ops, "", target))
        return temp_f
    return -1


# Check if  For Loop has if or any condition
def check_for_compare(node):
    for tmp in ast.walk(node):
        if isinstance(tmp, ast.Compare):
            return True
    return False


# Extract  function  then searches for loops in it. Store all loop information
def funtion_analysis(node):
    function_info = FunctionInformation()
    function_info.name = node.name
    for x in ast.walk(node):
        loop_information = extracted_loops(x)
        if loop_information != -1:
            function_info.iteration.append(loop_information)
    # for x in function_info.iteration:
    #     print(x.initial_line_number)
    #     print(x.final_line_number)
    #     print(x.allVariables)
    return function_info


# Walks for finding various functions
def program_analysis(program_information):
    with open("../examples/test.py", "rt") as fin:
        tree = ast.parse(fin.read())
    for x in ast.walk(tree):
        if isinstance(x, ast.FunctionDef):
            function_info = funtion_analysis(x)
            program_information.all_functions.append(function_info)


# Lets start codegen with no verification or anything just to test the my analysis works or not
def mapper_reducer_generation(program_information):
    list_of_new_operations = []
    s = ''
    initial_number = 0
    final_number = 0
    for functions in program_information.all_functions:
        for iteration in functions.iteration:
            initial_number = iteration.initial_line_number
            final_number = iteration.final_line_number
            for ops in iteration.operations:
                if isinstance(ops.op, ast.Add) and iteration.mainOperations == "ADD":
                    changeRDD = ops.left + '_RDD = sc.parallelize(' + 'numbers' + ')'
                    list_of_new_operations.append(changeRDD)
                    # s += ops.left + '=' + ops.left + '_RDD.map(lambda x:(1, x)).reduceByKey(lambda accum, num: accum + num).collect()'
                    s += ops.left + '=' + ops.left + '_RDD.sum()'
                    list_of_new_operations.append(s)
                    s = ''
                elif isinstance(ops.op, ast.Add) and iteration.mainOperations == "COUNT":
                    changeRDD = ops.left + '_RDD = sc.parallelize(' + 'numbers' + ')'
                    list_of_new_operations.append(changeRDD)
                    #s += ops.left + '=' + ops.left + '_RDD.map(lambda x:(1, x)).reduceByKey(lambda accum, _: accum +' + ops.right + ').collect()'
                    s += ops.left + '=' + ops.left + '_RDD.count()'
                    list_of_new_operations.append(s)
                elif isinstance(ops.op, ast.Lt) and iteration.mainOperations == "MAX":
                    changeRDD = ops.left + '_RDD = sc.parallelize(' + 'numbers' + ')'
                    list_of_new_operations.append(changeRDD)
                    # s += ops.left + '=' + ops.left + '_RDD.map(lambda x:(1, x)).reduceByKey(lambda accum, _: accum +' + ops.right + ').collect()'
                    s += ops.left + '=' + ops.left + '_RDD.max()'
                    list_of_new_operations.append(s)
                elif isinstance(ops.op, ast.Gt) and iteration.mainOperations == "MIN":
                    changeRDD = ops.left + '_RDD = sc.parallelize(' + 'numbers' + ')'
                    list_of_new_operations.append(changeRDD)
                    # s += ops.left + '=' + ops.left + '_RDD.map(lambda x:(1, x)).reduceByKey(lambda accum, _: accum +' + ops.right + ').collect()'
                    s += ops.left + '=' + ops.left + '_RDD.min()'
                    list_of_new_operations.append(s)
    return LoopReplace(initial_number, final_number, list_of_new_operations)


# Code Gen Portion
def codeGen(replace):
    fin = open("../examples/test.py", "rt")
    fout = open("../result/gen.py", "wt")
    cnt = 0
    fout.write("import pyspark as ps")
    for i, line in enumerate(fin):
        if not (replace.initial_line_no <= i+1  <= replace.final_line_number):
            fout.write(line)
        else:
            if cnt == 0:
                fout.write("    sc = ps.SparkContext()\n")
            if cnt < len(replace.replace_strings):
                fout.write("    " + replace.replace_strings[cnt] + "\n")
                cnt += 1


# check Number
def variable_check(operand):
    if isinstance(operand, ast.Name):
        return operand.id
    elif isinstance(operand, ast.Num):
        return operand.n


# check Variable
def main():
    program_information = ProgramInformation()
    program_analysis(program_information)
    replace = mapper_reducer_generation(program_information)
    codeGen(replace)
    for temp in program_information.all_functions:
        print(temp.name)


if __name__ == "__main__":
    main()
