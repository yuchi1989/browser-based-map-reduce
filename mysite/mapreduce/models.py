from django.db import models
from . import taskconfig
# Create your models here.

import sys,io,json, re
from . import jobscheduler
from django.http import HttpResponse
from django.views.decorators.csrf import csrf_exempt

@csrf_exempt
def gettask(request):
    response_data = {}
    print ("abc")    
    jobid = request.POST.get("jobid", "")
    param = {}
    param["action"] = 1
    param["jobid"] = int(jobid)
    param["mapcode"] = "abc"
    b1 = jobscheduler.Get_Job_Client(0,param)
    b1.connect()
    result = b1.run()
    print (result)
    if "result" in result:
        response_data['code'] = ""
        response_data['input'] = ""
        response_data['taskid'] = -1
    else:
        response_data['code'] = result["job"]["mapcode"]
        response_data['input'] = result["input"]
        response_data['taskid'] = result["taskid"]
    print ("getjob")
    return HttpResponse(json.dumps(response_data), content_type="application/json")

@csrf_exempt
def addtask(request):
    mapcode = request.POST.get("mapcode", "")
    reducecode = request.POST.get("reducecode", "")
    code1 = mapcode.encode('gbk','ignore').decode('gbk')
    print (code1)
    code2 = reducecode.encode('gbk','ignore').decode('gbk')
    print (code2)
    jobid = request.POST.get("jobid", "")
    print (jobid)
    inputfolder = request.POST.get("input", "")
    number_of_nodes = request.POST.get("nodes", "") 
    param = {}
    param["action"] = 0
    param["jobid"] = int(jobid)
    param["mapcode"] = mapcode
    reducecode = reducecode.replace(u'\xa0', ' ')
    reducecode = reducecode.replace('__reduce_function', '_JobServer__reduce_function ')
    reducecode = re.sub(r'(?<=[],a-z,:])\s\s\s\s',r'\n    ',reducecode)
    #reducecode = reducecode.replace(':', ':\n')
    param["reducecode"] = reducecode
    param["inputfolder"] = inputfolder
    param["nodes"] = int(number_of_nodes)
    a1 = jobscheduler.Add_Job_Client(0,param)
    a1.connect()
    result = a1.run()
    response_data = {}

    response_data['result'] = "addjob success"
    print ("addjob")
    return HttpResponse(json.dumps(response_data), content_type="application/json")

def getinput(request):
    return HttpResponse("getinput")

@csrf_exempt
def postreturn(request):
    taskid = request.POST.get("taskid", "")
    return_result = request.POST.get("result", "")
    jobid = request.POST.get("jobid", "")
    inputfolder = request.POST.get("input", "") 
    param = {}
    param["action"] = 2
    param["jobid"] = int(jobid)
    param["taskid"] = int(taskid)
    param["output"] = json.loads(return_result)
    a1 = jobscheduler.Add_Job_Client(0,param)
    a1.connect()
    result = a1.run()
    response_data = {}

    response_data['result'] = "postresult success"
    print ("return")
    return HttpResponse(json.dumps(response_data), content_type="application/json")

def setmap(request):
    return HttpResponse("setmapfunction")
