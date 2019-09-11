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
import gc
from joblib import dump, load
import redis as saver_c
import base64


# workarround to predict
# predict try to get model from redis memory
#   if not exist then download from s3 and update redis memory again for future processing
# workarround serialize
# binaryze the object and convert to b64 string
# save object to s3
# update redis memory

def mambaml_load_model(index):
    # this is a special function
    # all mambaml functions start with mambaml
    # this function locate the parent object
    # and get the model from memory instead of inputport
    # this function is usefule for predict endpoint
    # works in the same workarround
    pass


# joblib.load(io.BytesIO(new_file.read()))
def file_to_b64(filename):
    with open(filename, "rb") as f:
        binfile=f.read()
    b64_file = base64.b64encode(binfile)
    return b64_file

def save_c_file(filename, key):
    b64_file = file_to_b64(filename) 
    # one day
    # TODO  guardar en s3
    # save in cache
    saver_x.set(key, b64_file, ex=60*60*24)
    print("OBJECT SERIALIZED IN REDIS OK!!!!!")
    pass

def sk_learn_model_serialize_object_and_upload_to_s3(model, project_id, box_id):
    filename = 'sklearn_{}_{}.joblib'.format(str(project_id), box_id)
    dump(model, filename, compress = 1) 
    # TODO upload to s3 first if we have problems with redis
    save_c_file(filename, filename)
    os.remove(filename)
    # upload direct to s3??

def sk_learn_best_estimator_model_serialize_object_and_upload_to_s3(model, project_id, box_id):
    filename = 'sklearnbestestimator_{}_{}.joblib'.format(str(project_id), box_id)
    dump(model.best_estimator_, filename, compress = 1)
    save_c_file(filename, filename)
    os.remove(filename)
    # TODO upload to s3 first if we have problems with redis
    # upload direct to s3??

def c_run_str_code(inputs, str_code, depend, params):
#    return 'RUNED'
    global ret
    ret = []
    func_name = str_code.split('(')[0][4:].strip()

    sniped_code ="""
tmp_ret = FUNC_NAME(inputs) 
for x in tmp_ret:
    ret.append(x)"""
   
    sniped_code = sniped_code.replace('FUNC_NAME',func_name,1)
    parameters = None
    if not params is None and params != 'null':
        parameters = json.loads(params)

        for param in parameters:
            print("param-->", param)
            if param["type"] == "colselector":
                colsvalues = json.loads(param['value'])

                str_code = str_code.replace(param['name'], str(colsvalues["cols"]))
                print(str_code)
            else:
                str_code = str_code.replace(param['name'], param['value'])

    LOC = ""

    if depend:
        LOC = str_code +("\r\n")+depend+("\r\n")+ sniped_code
    else:
        LOC = str_code+ "\r\n" + sniped_code

    exec(LOC)

    return ret


class InputPort():

    def __init__(self, name, numport, parentBox, input_port_num):
        self.name = name
        self.numport = numport
        self.parentBox = parentBox
        self.input_port_num = input_port_num


class BoxCode():

    def __init__(self, str_code, box_id, n_inputs,
                 n_outputs, json, depend, params, changed,
                 project_id, host, serialize_outputs):
        self.project_id = project_id
        self.host = host
        self.str_code = str_code
        self.depend  = depend
        self.n_inputs = n_inputs
        self.n_outputs = n_outputs
        self.inputs = []
        self.outputs = []
        self.box_id = box_id
        self.setStatus('INIT')
        self.json = json
        self.params = params
        self.changed = changed
        self.serialize_outputs = serialize_outputs

    def clear_inputs(self):
        for x in self.inputs:
            del x
        self.inputs = []

    def setChangedBox(self, str_code, box_id, n_inputs, 
                      n_outputs, json, depend, params, changed, project_id, host, serialize_outputs):
        # to set trained false
        self.freespace()
        # if this box changed then result of preview run is empty
        self.json['nodes'][self.box_id]['properties']['payload']['result']=dict()
        # reset all parameter with new values
        self.__init__(str_code, box_id, n_inputs, n_outputs, json, depend,
                      params, changed, project_id, host, serialize_outputs)
        return None

    def isRunned(self):
        return self.status == 'RUNNED'    

    def getStatus(self):
        return self.status
 
    def setStatus(self, status):
        # TODO update get project status
        self.status = status
        requests.get(self.host+'/projects/set_status?id='+self.project_id+'&stat='+status+'&task='+self.box_id,)

        return self.status

    def freespace(self):
        for out in self.outputs:
            del out

        del self.outputs
        self.outputs = []

    def order_inputs(self):
        self.inputs.sort(key=lambda x: x.input_port_num, reverse=True)

    def run(self):
        try:
            if self.isRunned():
                return True
       
            self.setStatus('RUNNING')

            myinputs = []

            self.json['nodes'][self.box_id]['properties']['payload']['result']=dict()
            self.order_inputs()
            for i_input in self.inputs:
                i_input.parentBox.run()
                myinputs.append(i_input.parentBox.outputs[i_input.numport-i_input.parentBox.n_inputs])

            out = c_run_str_code(myinputs, self.str_code, self.depend, self.params)

            for p_out in out:
                self.outputs.append(p_out)
        # after run 
            index = 0
            # all box done ok
            print("AL--gooo")
            self.json['nodes'][self.box_id]['properties']['payload']['result']['status']="DONE_OK"
            for out in self.outputs:
                print("AL-Working with ouputs")
                self.json['nodes'][self.box_id]['properties']['payload']['result']['out'+str(index)]=dict()
                self.json['nodes'][self.box_id]['properties']['payload']['result']['out'+str(index)]['status'] = 'OK'

                if type(out) == type(pd.DataFrame()):
                    self.json['nodes'][self.box_id]['properties']['payload']['result']['out'+str(index)]['first100'] = out.head(100).to_json()
                    self.json['nodes'][self.box_id]['properties']['payload']['result']['out'+str(index)]['columns'] = pd.DataFrame(out.columns).to_json()
                index = index + 1
            # once trained ok then hasChange is False
            self.json['nodes'][self.box_id]['properties']['payload']['hasChange'] = False
            self.json['nodes'][self.box_id]['properties']['payload']['result']['error_message'] =''
            self.json['nodes'][self.box_id]['properties']['payload']['result']['error_args'] = ''
            self.json['nodes'][self.box_id]['properties']['payload']['result']['error_trace'] = ''
            
            print("OUTTTTT-->", self.serialize_outputs)

            if self.serialize_outputs!=None and self.serialize_outputs!=null and self.serialize_outputs != "[]":
                # then they have a serialized outputs
                s_outputs = json.loads(self.serialize_outputs)
                
                print("SSSSSOUT----->", s_outputs)
                for out in s_outputs:
                    print("OUT----->", out)
                    port_index = int(out['outputnum'])
                    port_type = out['Outputtype']
                    if port_type == 'sklearn_model' or port_type == 'sklean_model':
                       sk_learn_model_serialize_object_and_upload_to_s3(self.outputs[port_index], 
                                                                        self.project_id, 
                                                                        self.box_id)
                    elif port_type == 'GridSearch Model':
                       sk_learn_best_estimator_model_serialize_object_and_upload_to_s3(self.outputs[port_index], 
                                                                                       self.project_id, 
                                                                                       self.box_id)
                    else:
                        raise Exception(port_type + ' model type not allowed yet!')
            print("RUNNED")
            self.setStatus('RUNNED')
        except Exception as e:
            print("SOME ERROR")
            self.json['nodes'][self.box_id]['properties']['payload']['hasChange'] = True
            self.json['nodes'][self.box_id]['properties']['payload']['result']['status']="DONE_ERROR"
            self.json['nodes'][self.box_id]['properties']['payload']['result']['error_message'] = str(e)
            self.json['nodes'][self.box_id]['properties']['payload']['result']['error_args'] = ''
            self.json['nodes'][self.box_id]['properties']['payload']['result']['error_trace'] = traceback.format_exc()
            self.setStatus('ERROR')
            raise Exception('Run error check error message results!')
            return False
        return self.isRunned()


def run_celery_project(allboxes, project_id, task, host):

    # TODO do it better
    def getboxby_name(box_name, boxes):
        for x in boxes:
            if x.box_id == box_name:
                return x
        return None

    # TODO do it better
    def getportid_to_index(portid):
        return portid.split('port')[1]

    # free space fot all boxes we retrain all model
    def freespace(boxes):
        for box in boxes:
            box.freespace()
        gc.collect()

    # clear all inputs ports
    def clear_all_input_ports(boxes):
        for x in boxes:
            x.clear_inputs()

        gc.collect()

    d_json = ""
    try:

        valid_boxes = []
        if allboxes is None or task == 'ALL':
            if allboxes is not None:
                freespace(allboxes)
            allboxes=[]
            r = requests.get(host+'/projects/get_internal?id='+project_id)
            d_json = json.loads(r.json()['json'])
            for x in d_json['nodes']:
                box_type = d_json['nodes'][x]['type']
                if (box_type[0:7] == 'Dataset'):
                    node_name = x
                    import numpy as np
                    import pandas as pd
                    X = pd.DataFrame(np.array([[1, 1], [1, 2], [2, 2], [2, 3]]))
                    box = BoxCode("", node_name, 0, 1, d_json,"", "", False, project_id, host, "")
                    box.setStatus('RUNNED')
                    box.outputs.append(X)
                    # for now dummy data on dataset
                    allboxes.append(box)
                elif (box_type == ('Python Script')):
                    node_name = x
                    python_code = d_json['nodes'][x]['properties']['payload']['python_code']
                    n_inputs = d_json['nodes'][x]['properties']['payload']['n_input_ports']
                    n_outputs = d_json['nodes'][x]['properties']['payload']['n_output_ports']
                    depend = d_json['nodes'][x]['properties']['payload']['depen_code']
                    params = '' # input parameters
                    serialize_outputs = '' # output parameters to serialized
                    if 'parameters' in d_json['nodes'][x]['properties']['payload']:
                        params = d_json['nodes'][x]['properties']['payload']['parameters']
                    
                    if 'outputs' in d_json['nodes'][x]['properties']['payload']:
                        serialize_outputs = d_json['nodes'][x]['properties']['payload']['outputs']

                    box = BoxCode(python_code, node_name, n_inputs, n_outputs, 
                                  d_json, depend, params, False, project_id, host, serialize_outputs)
                    allboxes.append(box)

            for x in d_json['links']:
                orig_box_id = d_json['links'][x]['from']['nodeId']
                orig_input_port = d_json['links'][x]['from']['portId']
            
                dest_box_id = d_json['links'][x]['to']['nodeId']
                dest_input_port = d_json['links'][x]['to']['portId']

            # need locate orig_box
                orig_box = getboxby_name(orig_box_id, allboxes)
                orig_id = getportid_to_index(orig_input_port)
            
            # need locate dest_box
                dest_box = getboxby_name(dest_box_id, allboxes)
                dest_id = getportid_to_index(dest_input_port)
                if dest_box is not None and orig_box is not None and  dest_box.box_id != orig_box.box_id:
                    valid_boxes.append(dest_box)
                    valid_boxes.append(orig_box)
                        
                    input_port = InputPort(orig_input_port, int(orig_id)-1, orig_box, int(dest_id) - 1)

                    dest_box.inputs.append(input_port)

        else:
            if allboxes is None:
                allboxes = []

            r = requests.get(host+'/projects/get_internal?id='+project_id)
            d_json = json.loads(r.json()['json'])
            # DO it easy first we start with changed boxes
            # and existing boxes

            # at this point we can clear all input ports and recreate again
            # we trust in hasChange recibed from fronted
            clear_all_input_ports(allboxes)

            changedBox = []
            # this loop only analize changed boxes
            for x in d_json['nodes']:
                box = getboxby_name(x, allboxes)
                if box:
                    if ((d_json['nodes'][x]['properties']['payload']['hasChange'] == 'True') or
                        (d_json['nodes'][x]['properties']['payload']['hasChange'] == 'true') or
                        (d_json['nodes'][x]['properties']['payload']['hasChange'] == True)) :
                        node_name = x
                        python_code = d_json['nodes'][x]['properties']['payload']['python_code']
                        n_inputs = d_json['nodes'][x]['properties']['payload']['n_input_ports']
                        n_outputs = d_json['nodes'][x]['properties']['payload']['n_output_ports']
                        depend = d_json['nodes'][x]['properties']['payload']['depen_code']
                        params = ''
                        serialize_utputs = ''
                        if 'parameters' in d_json['nodes'][x]['properties']['payload']:
                            params = d_json['nodes'][x]['properties']['payload']['parameters']

                        if 'outputs' in d_json['nodes'][x]['properties']['payload']:
                            serialize_outputs = d_json['nodes'][x]['properties']['payload']['outputs']

                        box.setChangedBox(python_code, node_name, n_inputs, n_outputs,
                                          d_json, depend, params, False, project_id, host, serialize_outputs)

                        # save changed box and existing maybe need latter
                        changedBox.append(box)
                        print('Caja cambiada')
            
            # this loop analize new boxes
            # existing boxes and NOT changed then nothing to do
            newboxes = []
            for x in d_json['nodes']:
                box = getboxby_name(x, changedBox)
                box_exit = getboxby_name(x, allboxes)
                if box is None and box_exit is None:
                    # we can continue
                    node_name = x
                    python_code = d_json['nodes'][x]['properties']['payload']['python_code']
                    n_inputs = d_json['nodes'][x]['properties']['payload']['n_input_ports']
                    n_outputs = d_json['nodes'][x]['properties']['payload']['n_output_ports']
                    depend = d_json['nodes'][x]['properties']['payload']['depen_code']
                    params = ''
                    serialize_outputs = ''
                    if 'parameters' in d_json['nodes'][x]['properties']['payload']:
                        params = d_json['nodes'][x]['properties']['payload']['parameters']

                    if 'outputs' in d_json['nodes'][x]['properties']['payload']:
                        serialize_outputs = d_json['nodes'][x]['properties']['payload']['outputs']

                    box = BoxCode(python_code, node_name, n_inputs, n_outputs,
                                  d_json, depend, params, False, project_id, host, serialize_outputs)
                    allboxes.append(box)
                    newboxes.append(box)
                else:
                    print("Nothing to do")

            # at this point we can regenerate all links again
            for x in d_json['links']:
                orig_box_id = d_json['links'][x]['from']['nodeId']
                orig_input_port = d_json['links'][x]['from']['portId']

                dest_box_id = d_json['links'][x]['to']['nodeId']
                dest_input_port = d_json['links'][x]['to']['portId']

            # need locate orig_box
                orig_box = getboxby_name(orig_box_id, allboxes)
                orig_id = getportid_to_index(orig_input_port)

            # need locate dest_box
                dest_box = getboxby_name(dest_box_id, allboxes)
                dest_id = getportid_to_index(dest_input_port)
                if dest_box is not None and orig_box is not None and  dest_box.box_id != orig_box.box_id:
                    valid_boxes.append(dest_box)
                    valid_boxes.append(orig_box)

                    input_port = InputPort(orig_input_port, int(orig_id)-1, orig_box)
                    dest_box.inputs.append(input_port)

            # now need analize changedboxes and set run status as 'init'
            # this maybe can do better algoritm
            hasmore = True
            analizedbox = []
            while hasmore:
                hasmore = False
                for box in allboxes:
                    for inputlink in box.inputs:
                        if ((getboxby_name(inputlink.parentBox.box_id, changedBox) or 
                             getboxby_name(inputlink.parentBox.box_id, newboxes)) and 
                            (getboxby_name(box.box_id, analizedbox) is None)):
                           # then this box need to be re-run
                           # this box not changed but need to be retrained some parent change
                           # or some parent is new box
                               box.setStatus('INIT')
                               changedBox.append(box)
                               hasmore = True
                               analizedbox.append(box)
                               break
        
        # we can remove all empty boxes not conected with input
        # and outputs
        for x in allboxes:
            if getboxby_name(x.box_id, valid_boxes) is None:
                x.setStatus('STAND-BY')
        
        if task == 'ALL':
            # if we retrain ALL then clean an retrain
            pendingTrain = True
            while pendingTrain:
                pendingTrain=False
                for x in allboxes:
                    if ((x.isRunned()==False) and 
                        (not getboxby_name(x.box_id,valid_boxes) is None)):
                        x.run()
                        pendingTrain=True
        else:
            print("TRAY TO TRAIN TASK -->", task)
            to_trainbox = getboxby_name(task, allboxes)
            to_trainbox.run()
            # set in standby all boxes init
            # means user changed but not need to run becausethey want to run another circuit
            for x in allboxes:
                if x.getStatus() == 'INIT':
                    print("SET STAND-BY")
                    x.setStatus('STAND-BY')
            print("END TO TRAIN ONE BOX")
        url = host+'/projects/set_status?id='+project_id+'&stat=OK&error=NONE'+'&data='+json.dumps(d_json)
        print("URLLLLL-->", url)
        requests.get(url)
    except Exception as e:
        for x in allboxes:
            if x.getStatus() == 'INIT':
                print("SET STAND-BY")
                x.setStatus('STAND-BY')
        url = host+'/projects/set_status?id='+project_id+'&stat=ERROR'+'&error='+str(e)+'&data='+json.dumps(d_json)

        requests.get(url)
        print("END WITH ERROR", e)
        traceback.print_exc()

    return allboxes

def get_key_from_redis(project_id):
    r = requests.get(host+'/projects/get_status?id='+project_id)
   
    ret = r.json()    
    print(ret['status'])
    d_json = json.loads(ret['status'])
    print(d_json)
    if d_json['project_stat'] == 'PENDING':
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
print(sys.argv[3])
saver_x = saver_c.Redis(host=sys.argv[3], port=6379, db=0)

print("Starting project", project_id, host)
while True:
    print("ASK")
    try:
        task=get_key_from_redis(project_id)
        print("aaa", task)
        if task:
            print("NEW TASK", host)
            allproject = run_celery_project(allproject, project_id, task, host)
        else:
            print("NOTHING TO DO", task)
        time.sleep(1)
    except Exception as e:
        print('Error', e)
        time.sleep(1)
        pass
