from modules.nested_loop import NestedLoop
import ast

class ProgramInformation:
    filepath = ""

    def __init__(self):
        self.all_functions = []
        self.fileinfo = []

    def set_filepath(self, filepath):
        self.filepath = filepath

    def get_function_node_by_name(self, name):
        
        for tmp in self.all_functions:
            if tmp.name == name:
                return tmp.node
    
    def get_function_node_by_name_func(self, name):
        
        for tmp in self.all_functions:
            if tmp.name == name:
                return tmp



# Store all file reading information

class FileInfo:
    def __init__(self,  initial_line_number: int, final_line_number: int):
        self.fileanme = ""
        self.target = ""
        self.mode = ""
        self.initial_line_number = initial_line_number
        self.final_line_number = final_line_number

    def setFilename(filename):
        self.fileanme = filename
    def setTarget(target):
        self.target = target
    def setTarget(mode):
        self.mode = mode

# Store all function information
class FunctionInformation:

    def __init__(self, node):
        self.name = ""
        self.iteration = []
        self.input_variable = []
        self.return_variable = []  # TODO:  add return type for generation
        self.node = node
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
        self.has_nested_loops = False
        self.nested_loop_info = NestedLoop()

    def add_conditions(self, condition):
        self.conditions.append(condition)

    def add_operations(self, operation):
        self.operations.append(operation)

    def add_variables(self, variable):
        self.allVariables.append(variable)

    def add_comapre_information(self, compare_info):
        self.compareInformation.append(compare_info)

    def change_nested_loop(self, val: bool):
        self.has_nested_loops = val

    def check_for_nested(self, node: ast.For)->bool:
        for tmp in ast.walk(node):
            if isinstance(tmp, ast.For):
                return True
        return False

# Operation information stored in (left, op,  right)
class OperationInformation:

    def __init__(self, left, op, right, target, main_ops):
        self.left = left
        self.op = op
        self.right = right
        self.target = target
        self.ops = main_ops


# Stores all if/else condition for us
class CompareInformation:

    def __init__(self, left, ops, compare):
        self.left = left
        self.ops = ops
        self.compare = compare


# Stores all replace line information
class LoopReplace:
    def __init__(self, initial_line_no, final_line_number, replace_strings, filemarker =0):
        self.initial_line_no = initial_line_no
        self.final_line_number = final_line_number
        self.replace_strings = replace_strings
        self.filemarker = filemarker
    def setLoopReplace(self, initial_line_no, final_line_number, replace_strings):
        self.initial_line_no = initial_line_no
        self.final_line_number = final_line_number
        self.replace_strings = replace_strings
        self.filemarker = filemarker



# Stores Binary Operators

class BinOps:
    left = ""
    right = ""
    target = ""
    operation = ""
    operator = ""

    def __init__(self, left="", right="", target="", operations="", operator=""):
        self.left = left
        self.right = right
        self.target = target
        self.operation = operations
        self.operator = operator


# FOR UDF we need to search for summary and grammar
class SearchConfig:
    def __init__(self):
        self.inbits = 2
        self.arraySize = 4
        self.intRange = 4
        self.loopUnrolled = 4

        self.maxMR = 5
        self.maxEmits = 5
        self.maxTuple = 5
        self.maxRecursionDept = 5


# Variable Information
class VariableInformation:
    def __init__(self, varName, varType):
        self.varName = varName
        self.varType = varType
