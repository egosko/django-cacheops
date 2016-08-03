# coding: utf-8
from django.http import HttpResponse

from .models import Category


def category_cache_view(request):
    cnt = Category.objects.cache().count()
    return HttpResponse(cnt)