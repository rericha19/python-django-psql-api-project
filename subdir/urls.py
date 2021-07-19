from django.urls import path
from . import views

urlpatterns = [
    path('health', views.health_print, name = 'index'),
    path('ov/submissions/', views.submissions, name = 'submissions'),
    path('ov/submissions', views.submissions, name = 'submissions'),
    path('ov/submissions/<int:sub_id>', views.sub_delete),
    path('companies/', views.z3)
]
