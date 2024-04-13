import ast
import sys
from modules.models import LoopReplace

daskbag_import = "daskbag"
writemarker = 0


def processParams(args):

    workers = 32
    #partition = 100
    # if parameters are not passed, default settings will be used
    if(len(args)>=1):
        workers = int(args[0])
        #partition = int(args[1])
     
    return workers

# Lets start codegen with no verification or anything just to test the my analysis works or not
def mapper_reducer_generation(program_information):

    list_of_new_operations = []
    s = ''
    initial_number = 0
    final_number = 0

    postfix = "_DASK"
    
    fixvariable = "numbers"
    
    retval=[]

    bagimport = daskbag_import
    inputfilename = "input.csv"
    daskread = ".read_text('" + inputfilename +"',blocksize=blocksize).str.strip()\n"
    daskmap = ".map(lambda x: [int(num) for num in x.split(',')])\n"
    daskflatten = ".flatten()\n\n"

    # read file info
    for files in program_information.fileinfo:
        pass
        #s= files.target + " = list(" + daskbag_import + ".read_text(\'"+files.filename+"\').str.strip(\'\').str.split(\',\'))"
        #list_of_new_operations.append(s)
        
        #return LoopReplace(files.initial_line_number, files.final_line_number, list_of_new_operations,1)

    for functions in program_information.all_functions:

        for i in range(0,24):
            
            i+=1
       
        for iteration in functions.iteration:
           
            if iteration is not None:
                initial_number = iteration.initial_line_number
                final_number = iteration.final_line_number
                
                
                for ops in iteration.operations:
                                      
                    if (isinstance(ops.op, ast.Add) or isinstance(ops.op, ast.Sub)) and ops.ops == "ADD":
                       
                        #changeDASK = ops.left + postfix + '= ' + daskbag_import + '.from_sequence(list(' + fixvariable + '))'
                        #changeDASK = ops.left + postfix + '= ' + daskbag_import + '.from_sequence(list(' + fixvariable + '))'
                        changeDASK = ops.left + postfix +" = "+ bagimport + daskread
                        changeDASK += "\t" + ops.left + postfix +" = " + ops.left + postfix +  daskmap
                        changeDASK += "\t" + ops.left + postfix +" = " + ops.left + postfix +  daskflatten
                        
                        print(functions.return_variable)
                        list_of_new_operations.append(changeDASK)
                        
                        s += "with Client(n_workers=workers) as client:\n\t\t" + ops.left + '=' + ops.left + postfix + '.sum().compute()'
                        #s += ops.left + '=' + ops.left + postfix + '.sum().compute()'
                        list_of_new_operations.append(s)

                        if isinstance(ops.op, ast.Sub):
                            s=ops.left + '='+ops.left + '*-1'
                            list_of_new_operations.append(s)

                        s = ''
                    elif isinstance(ops.op, ast.Add) and ops.ops == "COUNT":
                        changeDASK = ops.left + postfix +" = "+ bagimport + daskread
                        changeDASK += "\t" + ops.left + postfix +" = " + ops.left + postfix +  daskmap
                        changeDASK += "\t" + ops.left + postfix +" = " + ops.left + postfix +  daskflatten

                        #changeDASK = ops.left + postfix + '= ' + daskbag_import + '.from_sequence(list(' + fixvariable + '))'
                        list_of_new_operations.append(changeDASK)
                        
                        s += "with Client(n_workers=workers) as client:\n\t\t" + ops.left + '=' + ops.left + postfix + '.count().compute()'
                        #s += ops.left + '=' + ops.left + postfix + '.count().compute()'
                        #changeRDD = ops.left + '_RDD = sc.parallelize(' + 'numbers' + ')'
                        #list_of_new_operations.append(changeRDD)
                        #s += ops.left + '=' + ops.left + '.count()'
                        list_of_new_operations.append(s)
                        s = ''
                    elif isinstance(ops.op, ast.Lt) and ops.ops == "MIN":
                        
                        changeDASK = ops.left + postfix +" = "+ bagimport + daskread
                        changeDASK += "\t" + ops.left + postfix +" = " + ops.left + postfix +  daskmap
                        changeDASK += "\t" + ops.left + postfix +" = " + ops.left + postfix +  daskflatten

                        #changeDASK = ops.left + postfix + '= ' + daskbag_import + '.from_sequence(list(' + fixvariable + '))'
                        list_of_new_operations.append(changeDASK)
                        s += "with Client(n_workers=workers) as client:\n\t\t" + ops.left + '=' + ops.left + postfix + '.min().compute()'
                        #changeRDD = ops.left + '_RDD = sc.parallelize(' + 'numbers' + ')'
                        #list_of_new_operations.append(changeRDD)
                        #s += ops.left + '=' + ops.left + '.max()'
                        list_of_new_operations.append(s)
                        s = ''
                    elif isinstance(ops.op, ast.Gt) and ops.ops == "MAX":
                        
                        #changeDASK = ops.left + postfix + '= ' + daskbag_import + '.from_sequence(list(' + fixvariable + '))'
                        
                        changeDASK = ops.left + postfix +" = "+ bagimport + daskread
                        changeDASK += "\t" + ops.left + postfix +" = " + ops.left + postfix +  daskmap
                        changeDASK += "\t" + ops.left + postfix +" = " + ops.left + postfix +  daskflatten

                        
                        list_of_new_operations.append(changeDASK)
                        s += "with Client(n_workers=workers) as client:\n\t\t" + ops.left + '=' + ops.left + postfix + '.max().compute()'
                        #changeRDD = ops.left + '_RDD = sc.parallelize(' + 'numbers' + ')'
                        #list_of_new_operations.append(changeRDD)
                        #s += ops.left + '=' + ops.left + '.min()'
                        list_of_new_operations.append(s)
                        s = ''

                    elif (isinstance(ops.op, ast.Add) or isinstance(ops.op, ast.Sub)) and ops.ops == "AVG":

                       
                        
                        #changeDASK = ops.left + postfix + '= ' + daskbag_import + '.from_sequence(list(' + fixvariable + '))'
                        
                        changeDASK = ops.left + postfix +" = "+ bagimport + daskread
                        changeDASK += "\t" + ops.left + postfix +" = " + ops.left + postfix +  daskmap
                        changeDASK += "\t" + ops.left + postfix +" = " + ops.left + postfix +  daskflatten

                        
                        list_of_new_operations.append(changeDASK)
                        s += "with Client(n_workers=workers) as client:\n\t\t" + ops.left + '=' + ops.left + postfix + '.mean().compute()'
                        #changeRDD = ops.left + '_RDD = sc.parallelize(' + 'numbers' + ')'
                        #list_of_new_operations.append(changeRDD)
                        #s += ops.left + '=' + ops.left + '.min()'
                        list_of_new_operations.append(s)
                        if len(functions.return_variable)>=1:                            
                            s= "return "+ functions.return_variable[0]
                            list_of_new_operations.append(s)
                    s = ''


    #retval.append(LoopReplace(initial_number, final_number, list_of_new_operations))
    #return retval
    return LoopReplace(initial_number, final_number, list_of_new_operations,0)


def write_all(replace, fout,writemarker):
    
    for s in replace:
        if writemarker==1:
            fout.write(s + "\n")
            writemarker=0
        else:
            fout.write("    " + s + "\n")


# Code Gen Portion
def codeGen(replace, filepath):
    fin = open(filepath, "rt")
    
    fout = open("../result/gen.py", "wt")
    cnt = 0
    #fout.write("import pyspark as ps\n")

    fout.write("import dask.bag as "+ daskbag_import + " \n\n")
    fout.write("from dask.distributed import Client\n")
    fout.write("import json\n")

    args = sys.argv[1:]    
    # read the dask configuration parameters
    workers = processParams(args)

    
    fout.write("workers="+ str(workers)+"\n")
    #fout.write("partition="+ str(partition)+"\n")
    fout.write("blocksize = '256MB'\n")

    
    for i, line in enumerate(fin):
        
            if not (replace.initial_line_no <= i + 1 <= replace.final_line_number):
                fout.write(line)
            else:
                if cnt == 0:
                    #fout.write("    sc = ps.SparkContext()\n")
                    write_all(replace.replace_strings,fout,replace.filemarker)
                    cnt += 1
