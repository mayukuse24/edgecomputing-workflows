from Component import Component
import random

class component_node:
    comp = None
    next_set = []
    data = []
    id = -1
    nodeid = -1
    order_number=10000000 # 1 + max(previous nessisary value) 0 for data source feed comps
    expected_inputs =1
    stat = False
    def __init__(self,comp,id=-1,next_set=[],expected_inputs=1,order_number=10000000):
        self.comp = comp
        self.id = id
        self.next_set = next_set
        self.expected_inputs = expected_inputs
        self.order_number=order_number
        self.stat = False


    def set_nodeid(self, used_ids):
        random_id = random.randint(10000, 65500)
        while random_id in used_ids:
            random_id = random.randint(10000, 65500)
        used_ids.append(random_id)
        self.nodeid = random_id
        return self.nodeid


    def give_data(self,data):
        if self.comp.input_type== 3 and len(self.data) ==1:
            self.data = [(convert(self.data[0][1],3,self.data[0][0],data[0],data[1]),3)]
        else:
            self.data =[data]


def convert_s2s_t(type1, type2, data:dict)-> dict:
    #dat =""
    name =""
    print("wwwwwwwwwwwwwwwwww",data)
    if type1 in [0,2]:
        if type1 == 2:
            datas = data["text"]
        else:
            datas = data["data"]
        #print(tmp)
        #dat = tmp[0][1]
        if type2 == 0:
            name = 'data'
            dat = datas
        elif type2 == 2:
            name = "text"
            dat = datas
        elif type2 == 7:
            name = "text"
            dat =[datas]
        else:
            return None
    return {name:dat}


def convert(type1, type2, data, data2=None, type3=-1) -> dict:
    if data2 is None:
        if type1 == type2:
            return data
        elif type1 in [0,2] and type2 in [0,2,7]:
            return convert_s2s_t(type1,type2,data)
        elif type2 ==4:
            if type1 == 2:
                datas = data["text"]
            else:
                datas = data["data"]
            return {"type":"gzip","data":datas}
    else:
        if type3 == 2:
            datas = data2["text"]
        else:
            datas = data2["data"]
        return {"text":datas,"audio":data}

class flow:
    start_components = [] #all the components feed directly by the data source
    run_order = []
    CONVERSION_MAP = {2:[0]}
    flow_id =-1
    data =[]
    run_items =0

    def __init__(self):
        start_components = []
        run_order = []


    def check_starers(self):
        input_types ={}
        for cmp in self.start_components:
            input_types.add(Component(component_node(cmp).comp).input_type)
        if len(input_types) >1:
            tmp = input_types
            for type in tmp:
                for a in self.CONVERSION_MAP[type]:
                    input_types.remove(a)

            if len(input_types) > 1:
                return "ERROR Starting components cannon be fead by single source"
        return 0

    def check_flow(self):
        comp_lst = self.start_components
        count=0
        while count<len(comp_lst):
            curr_comp = comp_lst[count]
            tmp_lst = curr_comp.next_set
            if len(tmp_lst)<1:
                continue
            comp_lst += tmp_lst
            out = [curr_comp.comp.output_type]
            out += self.CONVERSION_MAP[out[0]]
            for nxt_cmp in tmp_lst:
                if nxt_cmp.comp.input_type in out:
                    continue
                else:
                    return "ERROR the input of a component does not match the output of its source"
        return 0

