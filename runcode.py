import os
import json
import pandas as pd
import numpy as np
from sklearn.linear_model import LinearRegression
import traceback
import time
import sys
import requests
import traceback


def c_run_str_code(inputs, str_code, depend):
#    return 'RUNED'
    global ret
    ret = []
    print("START RUNNING CLIENT CODE")
    func_name=str_code.split('(')[0][4:].strip()

    sniped_code ="""
tmp_ret = FUNC_NAME(inputs) 
for x in tmp_ret:
    ret.append(x)"""
   
    sniped_code = sniped_code.replace('FUNC_NAME',func_name,1)
    LOC = ""
    if depend:
        LOC = depend + str_code + sniped_code
    else:
        LOC = str_code + sniped_code
    print(LOC)
    print("END RUN CLIENT CODE")    
    exec(LOC)

    return ret


class InputPort():

    def __init__(self, name, numport, parentBox):
        self.name = name
        self.numport = numport
        self.parentBox = parentBox


class BoxCode():

    def __init__(self, str_code, box_id, n_inputs, n_outputs, json, depend):
        self.str_code = str_code
        self.depend  = depend
        self.n_inputs = n_inputs
        self.n_outputs = n_outputs
        self.inputs = []
        self.outputs = []
        self.box_id = box_id
        self.status = 'INIT'
        self.json = json

    def isRunned(self):
        return self.status == 'RUNNED'    

    def getStatus(self):
        return self.status
 
    def setStatus(self, status):
        self.status = status
        return self.status

    def run(self):
        print("Start running", self.box_id, self.isRunned())
        try:
            if self.isRunned():
                return True
       
            self.setStatus('RUNNING')

            myinputs = []

            self.json['nodes'][self.box_id]['properties']['payload']['result']=dict()

            for i_input in self.inputs:
                i_input.parentBox.run()
                print("NUM TOTAL OUTS", len(i_input.parentBox.outputs))
                print("BUSCANDO INPUT PORT", i_input.numport)
                myinputs.append(i_input.parentBox.outputs[i_input.numport-i_input.parentBox.n_inputs])

            out = c_run_str_code(myinputs, self.str_code, self.depend)

            for p_out in out:
                print("añadiendo out", p_out)
                self.outputs.append(p_out)
        # after run 
            index = 0
            for out in self.outputs:
                print(type(self.json))
                self.json['nodes'][self.box_id]['properties']['payload']['result']['out'+str(index)]=dict()
                self.json['nodes'][self.box_id]['properties']['payload']['result']['out'+str(index)]['status'] = 'OK'
                if type(out) == type(pd.DataFrame()):
                    self.json['nodes'][self.box_id]['properties']['payload']['result']['out'+str(index)]['first100'] = out.head(100).to_json()
                    self.json['nodes'][self.box_id]['properties']['payload']['result']['out'+str(index)]['columns'] = out.columns.to_json()

                index = index + 1
            self.setStatus('RUNNED')
        except Exception as e:
            self.json['nodes'][self.box_id]['properties']['payload']['result']['error_message'] = str(e)
            self.json['nodes'][self.box_id]['properties']['payload']['result']['error_args'] = ''
            self.json['nodes'][self.box_id]['properties']['payload']['result']['error_trace'] = traceback.print_exc()
            raise Exception('Run error check error message results!')
            return False
        print("End running", self.box_id, self.isRunned())
        return self.isRunned()


def run_celery_project(allboxes, project_id, task, host):

    # REPLACE THIS QUERY FOR NEW ENDPOINT TO BACKEND TO RETRIVE ALL JSON DATA FROM PROJECT
    # project = Project.query.filter(Project.id == project_id).first()
    
    # r = requests.get('https://api.github.com/user', auth=('user', 'pass'))
    
    print("START CELERY TASK")
    d_json = ""
    try:
        if allboxes is None:
            allboxes=[]
            # json_wf = project.json
            # d_json = json.loads(json_wf)
            # print(type(d_json))
            r = requests.get(host+'/projects/get_internal?id='+project_id)
            # print(r)
            d_json = json.loads(r.json()['json'])
            # print(d_json)
            # allboxes = []
            #for x in d_json['nodes']:
            #    print("AAAA", x)
            for x in d_json['nodes']:
#                print("milog",d_json['nodes'][0]['type'])
                box_type = d_json['nodes'][x]['type']
                print(box_type)
                if (box_type[0:7] == 'Dataset'):
                    # node_name = d_json['nodes'][x]['id']
                    node_name = x
                    # TODO OK for now special case for dataset!i
                    # regressor = DecisionTreeRegressor(random_state=0)
                    # cross_val_score(regressor, boston.data, boston.target, cv=10)                  
                    import numpy as np
                    # from sklearn.linear_model import LinearRegression
                    X = np.array([[1, 1], [1, 2], [2, 2], [2, 3]])
                    box = BoxCode("", node_name, 0, 1, d_json,"")
                    box.setStatus('RUNNED')
                    box.outputs.append(X)
                    # for now dummy data on dataset
                    allboxes.append(box)
                elif (box_type == ('Python Module-Python Script')):
                    # TODO añadir box_type categoria sub categoria etc...
                    # se podrian llamar igual pero pertencer a otra categria
                    # no es importante pero si se pued mejor...
                    # O ES UN ENDPOINT
                    # node_name = d_json['nodes'][x]['id']
                    node_name = x
                    print(node_name)
                    python_code = d_json['nodes'][x]['properties']['payload']['python_code']
                    n_inputs = d_json['nodes'][x]['properties']['payload']['n_input_ports']
                    n_outputs = d_json['nodes'][x]['properties']['payload']['n_output_ports']
                    depend = d_json['nodes'][x]['properties']['payload']['depen_code']
                    box = BoxCode(python_code, node_name, n_inputs, n_outputs, d_json, depend)
                    allboxes.append(box)

        # TODO do it better
            def getboxby_name(box_name):
                for x in allboxes:
                    if x.box_id == box_name:
                        return x
                return None
            # TODO do it better
            def getportid_to_index(portid):
                return portid.split('port')[1]

            for x in d_json['links']:
                orig_box_id = d_json['links'][x]['from']['nodeId']
                orig_input_port = d_json['links'][x]['from']['portId']
            
                dest_box_id = d_json['links'][x]['to']['nodeId']
                dest_input_port = d_json['links'][x]['to']['portId']

            # need locate orig_box
                orig_box = getboxby_name(orig_box_id)
                orig_id = getportid_to_index(orig_input_port)
            
            # need locate dest_box
                dest_box = getboxby_name(dest_box_id)
                print(dest_box_id)
                dest_id = getportid_to_index(dest_input_port)
                if dest_box.box_id != orig_box.box_id:
                    input_port = InputPort(orig_input_port, int(orig_id)-1, orig_box)
                    dest_box.inputs.append(input_port)


        # now select one untrained box and train until allboxes trained
        # TODO IN random way
        pendingTrain = True
        while pendingTrain:
            pendingTrain=False
            for x in allboxes:
                if x.isRunned()==False:
                    x.run()
                    pendingTrain=True

        # print("BOX_ID-->",box_id)
        requests.get(host+'/projects/set_status?id='+project_id+'&data='+json.dumps(d_json)+'&stat=OK&error=NONE')
        print("END OK")
    except Exception as e:
        requests.get(host+'/projects/set_status?id='+project_id+'&data=NONE&stat=ERROR'+'&error='+str(e))
        print("END WITH ERROR", e)
        traceback.print_exc()

    return allboxes

def get_key_from_redis(project_id):
    r = requests.get(host+'/projects/get_status?id='+project_id)
   
    ret = r.json()    
    print(ret)
    if ret['status'] == 'PENDING':
        return ret['task']
        
    return False

def run_str_code(func):
    global ret
    ret = []
    LOC1 = """def """+func+"""(num):"""
    LOC = """
    num*2 
    print("hola")
    return num
"""
    exec(LOC1+LOC)
  
    return ret


allproject = None
project_id = sys.argv[1]
host = sys.argv[2]
print("Starting project", project_id, host)
while True:
    print("ASK")
    try:
        task=get_key_from_redis(project_id)
        if task:
            print("NEW TASK")
            allproject = run_celery_project(None, project_id, task, host)
        else:
            print("NOTHING TO DO", task)
        time.sleep(1)
    except Exception as e:
        print(e)
        time.sleep(1)
        pass
