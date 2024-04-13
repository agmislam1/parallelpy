import ast
from modules.udf_models import CustomLoopInformation
from modules.codegen_udf import *


def udf_calls(mapfunc,node, call_type, final_target, program_info, intial_num, final_num, input_dataset="", map_operation=None):
    
   
    customNode = CustomLoopInformation(call_type, input_dataset, program_info)
    
    # Need to check
    #customNode.check_for_filter(node, "")
    
    
    
    type = customNode.classify_udf(node)

    
   
    if type == 0:
        
        final_gen = codegen_complete_mapper(mapfunc,final_target, customNode.mapper_list.mr_steps,"")
    elif type == 1:
        final_gen = codegen_complete_mapper_filter(final_target,customNode.final_codegen_value)
    elif type == 3:
        
        final_gen = codegen_complete_mapper(mapfunc,final_target, customNode.mapper_list.mr_steps, customNode.input_dataset,map_operation)
        #final_gen = codegen_complete_mapper(mapfunc,final_target, customNode.mapper_list.mr_steps, customNode.input_dataset,"")
    elif type == 5:
        
        final_gen = codegen_complete_reducer_multiple_mapper(final_target, customNode.parallilizeList)
    else:
        
        final_gen = codegen_complete_reducer(final_target,customNode.final_codegen_value, customNode.input_dataset)
    #removed print(*final_gen, sep="\n")
    code_gen_file(program_info.filepath, intial_num, final_num, final_gen, type, customNode.multipleMapCode)
    # customNode.getInputVariables(node)
    # customNode.getOperators(node)
    return

