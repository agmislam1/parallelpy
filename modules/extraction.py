
from modules.models import *
from modules.udf import udf_calls
from modules.codegen_udf import *
from modules.codegen import *
from modules.nested_loop import NestedLoop

from modules import  models

# Return First and Last Line number
def _compute_interval(node):
    min_lineno = node.lineno
    max_lineno = node.lineno
    for node in ast.walk(node):
        if hasattr(node, "lineno"):
            min_lineno = min(min_lineno, node.lineno)
            max_lineno = max(max_lineno, node.lineno)
    return min_lineno, max_lineno


# check Number
def variable_check(operand):
    if isinstance(operand, ast.Name):
        return operand.id
    elif isinstance(operand, ast.Num):
        return operand.n

def modify_lambda_expression(original_lambda_expr_str):
    # Parse the original lambda expression into an AST node
    original_lambda_expr_ast = ast.parse(original_lambda_expr_str)

    class LambdaModifier(ast.NodeTransformer):
        def visit_Lambda(self, node):
            # Check if it's the lambda expression we want to modify
            if isinstance(node, ast.Lambda) and isinstance(node.body, ast.Subscript) and isinstance(node.body.value, ast.Name) and node.body.value.id == 'x':
                # Modify the body of the lambda expression to unary negation
                node.body = ast.UnaryOp(op=ast.USub(), operand=node.body)
            return node

    # Instantiate the LambdaModifier class and transform the AST
    lambda_modifier = LambdaModifier()
    modified_ast = lambda_modifier.visit(original_lambda_expr_ast)

    # Convert the modified AST back to Python code representation
    modified_lambda_expr_str = ast.unparse(modified_ast)

    return modified_lambda_expr_str


    # Identify the sort method call inside the for loop
def findsortmethod(node, mapoperation):

    
    if isinstance(node.func, ast.Name) and node.func.id == 'sorted':
        mapoperation = "topk(k"
        #print(ast.unparse(node.args[0]))

        # appnd key value if exists
        for arg in node.keywords:
            if arg.arg == 'key':
                mapoperation += ",key = "+ modify_lambda_expression(ast.unparse(arg.value))
               
                #original_lambda_expr_ast = ast.parse(arg.value)
                

       
        mapoperation += ")"
        #print("Sorted method found")
        
        
        
    return mapoperation
        
    


# Extracts all loops from a function node



def extracted_loops(node, program_info : ProgramInformation, node2, f_info):
    aggregateType=0
    for x in ast.walk(node2):
            # Store return variable
            if isinstance(x, ast.Return):                
                #print(x.value.id)
                if(isinstance(x.value,ast.BinOp)):
                   
                    # indicates it is an average
                    aggregateType=1                   
                    f_info.return_variable.append(x.value.left.id)
    

    if isinstance(node, ast.For):
        # new addition
        map_operation = None
       
        min_line_no, max_line_no = _compute_interval(node)
        
        if aggregateType==1:
            max_line_no = max_line_no + 3
        temp_f = LoopInformation(min_line_no, max_line_no)   

        has_compare = check_for_compare(node)
        temp_f.change_nested_loop(temp_f.check_for_nested(node.body[0]))
        # FOR NESTED LOOP
        if temp_f.has_nested_loops:
            temp_f.nested_loop_info.data_1 = node.iter.id
            temp_f.nested_loop_info.check_join(node)
            if temp_f.nested_loop_info.is_join:
                temp_f.nested_loop_info.get_all_operation()
            
            
            codegen_list = temp_f.nested_loop_info.convert_operations_mapper_reducer()
            code_gen_file(program_info.filepath, min_line_no-1, max_line_no+1, codegen_list, 1)
            
            
            return -2
        else:

            for inner_node in ast.walk(node):
                if isinstance(inner_node, ast.Call) and map_operation is None:
                    map_operation = findsortmethod(inner_node,map_operation)
                    #sort_method_finder.visit(inner_node)
            
            for v_node in ast.walk(node):
               # print(node) get all variables
               # Line 45 - 59 comment
                
                

               ## check for udf call +,*,-,*,/
                if isinstance(v_node, ast.Assign):
                        
                        if isinstance(v_node.value, ast.BinOp):
                            
                            #print(assignmentInfo.targets[1].id)
                            left = variable_check(v_node.value.left)
                            op = v_node.value.op
                            right = variable_check(v_node.value.right)
                            target = v_node.targets[0].id
                            if target != left:
                                temp = left
                                left = right
                                right = temp
                            
                            if right == None and isinstance(op, ast.Add):
                                
                                map_operation = "sum"
                                
                    
                
                if isinstance(v_node, ast.Call) and v_node.func.id != "enumerate":
                    
                    funcName = v_node.func.id
                    assignmentInfo = node.body.pop(0)

                    
                    if isinstance(assignmentInfo.targets[0], ast.Name):
                        final_target = assignmentInfo.targets[0].id
                        
                        try:
                            input_dataset = node.iter.id
                        except:
                            input_dataset = "random" # TODO CHANGE THIS ONE AS WELL
                        #input_dataset = ""
                        
                       
                        #print(program_info.get_function_node_by_name(funcName))
                    
                        
                        udf_calls("",program_info.get_function_node_by_name(funcName), "rExpression",final_target, program_info, min_line_no, max_line_no, input_dataset,map_operation)
                        #print("---- map-reduce code generated----") 
                        
                    else:
                        final_target = assignmentInfo.targets[0].value.id

                        ## get the parameters of the function call for dask

                        funcinput = ""
                        mapfunction = ""
                        for i in range(len(v_node.args)):
                            if(i!=0):
                                funcinput+=v_node.args[i].id+","
                            ++i

                        if len(funcinput)>0:
                            if funcinput[len(funcinput)-1] == ",":
                                funcinput = funcinput[:-1]
                        mapfunc = funcName
                        
                        if len(funcinput)>0:
                            mapfunc = mapfunc + "," + funcinput

                       

                        # New parameter func_node added to the following call
                        
                        udf_calls(mapfunc,program_info.get_function_node_by_name(funcName), "mExpression", final_target, program_info, min_line_no, max_line_no,map_operation)
                        print("---- map-reduce code generated----") 
                    break
                if isinstance(v_node, ast.Name) and isinstance(v_node.ctx, ast.Store):

                    
                    if not temp_f.allVariables.__contains__(v_node.id):
                        #removed print(v_node.id)
                        temp_f.add_variables(v_node.id)
                # Only for Sum and Count
                if not has_compare:
                    
                    if isinstance(v_node, ast.Assign):
                        #removed print(v_node)
                        if isinstance(v_node.value, ast.BinOp):
                            left = variable_check(v_node.value.left)
                            op = v_node.value.op
                            right = variable_check(v_node.value.right)
                            target = v_node.targets[0].id
                            if target != left:
                                temp = left
                                left = right
                                right = temp
                            #removed print(left, op, right, target)
                            if right == "1" or right == 1:
                                temp_f.add_operations(OperationInformation(left, op, right, target, "COUNT"))
                                print("---- code translated----") 
                            else:
                                
                                if aggregateType==0:
                                    temp_f.add_operations(OperationInformation(left, op, right, target, "ADD"))
                                else:
                                    temp_f.add_operations(OperationInformation(left, op, right, target, "AVG"))
                                print("---- code translated----")
                                
                else:
                    
                    if isinstance(v_node, ast.If):
                        compare_info = CompareInformation(v_node.test.left.id, v_node.test.ops[0], v_node.test.comparators[0].id)
                        temp_f.add_comapre_information(compare_info)
                        #removed print(compare_info)
                        for ifbody in v_node.body:
                            if isinstance(ifbody, ast.Assign):
                                target = ifbody.targets[0].id
                                if isinstance(compare_info.ops,ast.Lt):
                                    temp_f.add_operations(OperationInformation(target, compare_info.ops, "", target,"MIN"))
                                else:
                                    
                                    temp_f.add_operations(OperationInformation(target, compare_info.ops, "", target,"MAX"))
                        print("---- code translated----")
        return temp_f
    return None

def extracted_loops1(node, program_info : ProgramInformation):

    

    if isinstance(node, ast.For):
        
        
        
        
        min_line_no, max_line_no = _compute_interval(node)
        

        temp_f = LoopInformation(min_line_no, max_line_no)
        has_compare = check_for_compare(node)
        temp_f.change_nested_loop(temp_f.check_for_nested(node.body[0]))

        for v_node in ast.walk(node):
            
            if not has_compare:
                        
                        
                        if isinstance(v_node, ast.Assign):
                            
                            if isinstance(v_node.value, ast.BinOp):
                                left = variable_check(v_node.value.left)
                                op = v_node.value.op
                                right = variable_check(v_node.value.right)
                                target = v_node.targets[0].id
                                if target != left:
                                    temp = left
                                    left = right
                                    right = temp
                               
                               #removed  print(left, op, right, target)
                                
                                if right == "1" or right == 1:
                                    temp_f.add_operations(OperationInformation(left, op, right, target, "COUNT"))
                                else:
                                    
                                    temp_f.add_operations(OperationInformation(left, op, right, target, "ADD"))
            else:
                                   
                                    if isinstance(v_node, ast.If):
                                        
                                        compare_info = CompareInformation(v_node.test.left.id, v_node.test.ops[0], v_node.test.comparators[0].id)
                                        temp_f.add_comapre_information(compare_info)
                                        
                                        for ifbody in v_node.body:
                                            if isinstance(ifbody, ast.Assign):
                                                target = ifbody.targets[0].id
                                                if isinstance(compare_info.ops,ast.Lt):
                                                    temp_f.add_operations(OperationInformation(target, compare_info.ops, "", target,"MIN"))
                                                else:
                                                    
                                                    temp_f.add_operations(OperationInformation(target, compare_info.ops, "", target,"MAX"))
        # print("------------I am done---------------")
        return temp_f
    return None

        





# Check if  For Loop has if or any condition
def check_for_compare(node):
    for tmp in ast.walk(node):
        if isinstance(tmp, ast.Compare):
            return True
    return False


# Extract  function  then searches for loops in it. Store all loop information
def funtion_analysis(node, progam_info):
    
    if isinstance(node, ast.FunctionDef):
        function_info = FunctionInformation(node)
        function_info.name = node.name
        function_info.input_variable.append(node.args.args)
        progam_info.all_functions.append(function_info)
        for x in ast.walk(node):
                        
            loop_information = extracted_loops(x, progam_info,node, function_info)
            if loop_information == -2:
                return
            # CHECK FOR NESTED LOOP HERE
            function_info.iteration.append(loop_information)
            #if loop_information != -1:
                #function_info.iteration.append(loop_information)
        # for x in function_info.iteration:
        #     print(x.initial_line_number)
        #     print(x.final_line_number)
        #     print(x.allVariables)

       
        return function_info

# Check for file reading block
def extracted_fileinfo(node, program_info : ProgramInformation):
    min_line_no, max_line_no = _compute_interval(node)
    filename = ""
    target = ""

    tempinfo = models.FileInfo(min_line_no,max_line_no)
    
    for params in node.items:
        # get the file name
        filename = params.context_expr.args[0].value
        tempinfo.filename = filename
        
        

    for items in node.body:
        # get the variable Name
        if isinstance(items, ast.Assign):
            target = items.targets[0].id
            tempinfo.target = target
            
    if filename!= "" and target != "":        
        
        program_info.fileinfo.append(tempinfo)
       

 
        
               

# Walks for finding various functions
def program_analysis(program_information : ProgramInformation, filepath):
    program_information.set_filepath(filepath)
    with open(filepath, "rt") as fin:
        tree = ast.parse(fin.read())
    for x in ast.walk(tree):


        
        #if isinstance(x, ast.With):
        #    extracted_fileinfo(x,program_information)

        if isinstance(x, ast.FunctionDef):
            function_info = funtion_analysis(x, program_information)



# Convert expression extracted to standard one
def convert_expression_to_standard():
    return ""
