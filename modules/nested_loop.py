import ast
from modules.udf_models import BinOps


class NestedLoop:

    def __init__(self):
        self.is_join = False
        self.join_key_index = ""
        self.operations = []
        self.data_1 = ""
        self.data_2 = ""
        self.code_generation = []
        self.ops_body = ast.If
        self.return_val = "dask_bag_result"
        # check Number

    def variable_check(self, operand):
        if isinstance(operand, ast.Name):
            return operand.id
        elif isinstance(operand, ast.Num):
            return operand.n

    def add_operations(self, bino):
        self.operations.append(bino)

    def check_join(self, node):
        for tmp in ast.walk(node):
            if isinstance(tmp, ast.For):
                self.data_2 = tmp.iter.id
            if isinstance(tmp, ast.If):
                self.is_join = True
                self.join_key_index = 0
                self.ops_body = tmp
                break
        self.code_gen_static()

    def code_gen_static(self):

        # filenames are passed for dask

        self.code_generation.append(self.data_1 + "_bag = daskbag.read_text('join1.csv',blocksize=blocksize).str.strip()")
        self.code_generation.append(self.data_1 + "_bag = " + self.data_1 + "_bag.map(lambda x: tuple(map(str, x.strip().split(',')))).map(lambda x: (x[0], int(x[1])))")

        self.code_generation.append("\n")
        
        self.code_generation.append(self.data_2 + "_bag = daskbag.read_text('join2.csv',blocksize=blocksize).str.strip()")
        self.code_generation.append(self.data_2 + "_bag = " + self.data_2 + "_bag.map(lambda x: tuple(map(str, x.strip().split(',')))).map(lambda x: (x[0], int(x[1])))")
        
        self.code_generation.append("\n")
        

        # Uncomment the below lines for pyspark
        #self.code_generation.append(self.data_1 + "_RDD = sc.parallelize(" + self.data_1 + ")")
        #self.code_generation.append(self.data_2 + "_RDD = sc.parallelize(" + self.data_2 + ")")
        if not self.is_join:

            # Code for dask bag
            self.code_generation.append(
                self.data_1 + "_bag_result =" + self.data_1 + "_bag.product(" + self.data_2 + "_bag)")
            #self.code_generation.append(self.return_val + " = bag_product.map(lambda x: (x[0][0], x[0][1] , x[1][1]))")

            

            # Uncomment the below line for pyspark
            #self.code_generation.append(
            #    self.data_1 + "_RDD_combine =" + self.data_1 + "_RDD.cartesian(" + self.data_2 + "_RDD)")
        else:

            # Code for Dask Bag
            self.code_generation.append(
                self.data_1 + "_bag_result =" + self.data_1 + "_bag.product(" + self.data_2 + "_bag)")
            #self.code_generation.append(self.return_val + " = bag_product.map(lambda x: (x[0][0], x[0][1] , x[1][1]))")

            # Uncomment the below line for pyspark
            #self.code_generation.append(
            #    self.data_1 + "_RDD_combine =" + self.data_1 + "_RDD.join(" + self.data_2 + "_RDD)")

    def get_all_operation(self):
        for tmp in ast.walk(self.ops_body):
            try:
                for a in tmp.body:
                    if isinstance(a.value, ast.BinOp):
                        left = self.variable_check(a.value.left)
                        op = a.value.op
                        right = self.variable_check(a.value.right)
                        # target = tmp_node.targets[0].id
                        print(left, op, right, "")
                        binary_operation = BinOps(left, right, "", op)
                        binary_operation.get_operation_from_operator()
                        self.add_operations(binary_operation)
            except:
                pass

    def convert_operations_mapper_reducer(self):

        
        #var1 = self.data_1 + "_RDD_combine =" + self.data_1+ "_RDD_combine"
        var1 = self.data_1 + "_bag_result"
        var3 = "result"
        

        for each_op in self.operations:
            tmp_code = var1 + " = " + var1 + ".filter(lambda x: x[0][0] == x[1][0]).map(lambda x: (x[0][0], x[0][1] + x[1][1]))"
            

            # Uncomment the below line for pyspark
            #tmp_code = var1+ " = "+ var1 + ".map(lambda x: (x[0],x[1][0]" + each_op.operation + "x[1][1])).collect()"
            self.code_generation.append(tmp_code)

        self.code_generation.append("with Client(n_workers=workers) as client:")
       

        if len(self.operations) != 0:

            self.code_generation.append("\t" + var3 + " = " + var1 + ".compute()")
            self.code_generation.append("return " + var3)
            # Uncomment the below line for pyspark
            #self.code_generation.append("return "+ self.data_1 + "_RDD_combine")
        else:

            # Code for Dask Bag
            self.code_generation.append("\t" + var3 + " = " + var1 + ".compute()")
            self.code_generation.append("return " + var3)
            # Uncomment the below line for pyspark
            #self.code_generation.append("return " + self.data_1 + "_RDD_combine.collect()")
        return self.code_generation

    
