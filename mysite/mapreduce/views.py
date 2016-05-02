from django.shortcuts import render

# Create your views here.
from django.http import HttpResponse
from django.shortcuts import render_to_response

def index(request):
    return render_to_response('get_job_page.html') 
def manage(request):
    return render_to_response('job_management_page.html') 
