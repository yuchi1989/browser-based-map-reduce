from django.conf.urls import url

from . import views
from . import models
urlpatterns = [
    url(r'^$', views.index, name='index'),
    url(r'^gettask$', models.gettask, name='gettask'),
    url(r'^addtask$', models.addtask, name='addtask'),
    url(r'^getinput$', models.getinput, name='getinput'),
    url(r'^postreturn$', models.postreturn, name='postreturn'),
    url(r'^setmapfunction$', models.setmap, name='setmap'),
    url(r'^manage$', views.manage, name='manage'),
]
