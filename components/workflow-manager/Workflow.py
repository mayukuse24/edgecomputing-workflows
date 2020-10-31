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


    def set_nodeid(self, used_ids):
        random_id = random.randint(10000, 65500)
        while random_id in used_ids:
            random_id = random.randint(10000, 65500)
        used_ids.append(random_id)

    def give_data(self,data):
        self.data +=[data]


class flow:
    start_components = [] #all the components feed directly by the data source
    run_order = []
    CONVERSION_MAP = {}
    flow_id =-1
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

