from django.conf.urls import url

from . import views

urlpatterns = [
    url(r'^category/$', views.category_cache_view, name="category_cache_view"),
]
