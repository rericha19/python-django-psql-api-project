from django.urls import path
from . import views_orm

urlpatterns = [
    path('ov/submissions', views_orm.submission),
    path('ov/submissions/', views_orm.submission),
    path('ov/submissions/<int:sub_id>', views_orm.submission_id),
    path('companies/', views_orm.companies)
]
