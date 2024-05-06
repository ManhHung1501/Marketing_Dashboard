"""
Definition of models.
"""

from os import defpath
from django.db import models
from django.db.models import fields
from django.db.models.enums import Choices
from django.db.models.fields import AutoField
from django.contrib.auth.models import User
from django.contrib import admin
import architect
# Create your models here.

class Country(models.Model):
    country_id = models.CharField(max_length = 255, primary_key=True)
    country_name = models.CharField(max_length = 255, unique= True)
    def __str__(self):
        return self.country_name
    def get_deferred_fields(self) -> country_id:
        return super().get_deferred_fields()

class Game(models.Model):
    GAME_STATUS = (('active', 'Active'), ('archived', 'Archived'), ('inactive', 'Inactive'))
    _id = models.AutoField(primary_key=True)
    name = models.CharField(max_length = 255)
    platform = models.CharField(max_length = 255, null=True, blank=True)
    id_bundle = models.CharField(max_length = 255, unique=True)
    creation_date = models.DateField(auto_now=False,auto_now_add=True)
    id_track = models.CharField(max_length = 255, null=True, blank=True)
    class Meta:
        indexes = [
            models.Index(fields=['name']),
            models.Index(fields=['platform']),
            models.Index(fields=['id_bundle']),
            models.Index(fields=['id_track']),
            ]
    def __str__(self):
        return self.name
    def get_deferred_fields(self) -> _id:
        return super().get_deferred_fields()
    def get_queryset(self, request):
        if request.user.get_username() == "admin":
            return Game.objects.all()
        else:
            return Game.objects.filter(user_can_view = User.objects.get(username = request.user.get_username()).id)

class Data_Appsflyer(models.Model):
    _id = models.AutoField(primary_key=True)
    product = models.ForeignKey(Game, to_field='id_bundle', on_delete=models.CASCADE)
    country = models.ForeignKey(Country, to_field='country_id', on_delete=models.CASCADE)
    network = models.CharField(max_length=255, default = 'Default')
    date_update = models.DateField()
    cost = models.FloatField(default = 0)
    installs = models.IntegerField(default = 0)
    activity_revenue = models.FloatField(default = 0)
    roi = models.FloatField(default = 0)
    avg_ecpi = models.FloatField(default = 0)
    uninstall_rate = models.FloatField(default = 0)
    platform = models.CharField(max_length = 50,default='android')

    class Meta:
        indexes = [
            models.Index(fields=['date_update']),
            models.Index(fields=['network']),
            models.Index(fields=['platform']),
            models.Index(fields=['date_update', 'product', 'country', 'platform']),
            ]

class Network(models.Model):
    _id = models.AutoField(primary_key=True)
    name = models.CharField(max_length=255, default = 'Applovin', unique=True)
    info = models.CharField(max_length = 1000, null = True, blank=True)
    parent = models.ForeignKey(User, to_field='username', on_delete=models.CASCADE, default='admin')
    class Meta:
        indexes = [
            models.Index(fields=['name']),
            ]
        verbose_name = 'Network'
        verbose_name_plural = 'Network Information'
    def __str__(self):
        return self.name
    def get_queryset(self, request):
        if request.user.get_username() == "admin":
            query = SourceKey.objects.all()
        else:
            query = SourceKey.objects.filter(parent=request.user)
        return query

class GameUser(models.Model):
    user = models.OneToOneField(User, to_field='username', on_delete = models.CASCADE)
    game = models.ManyToManyField(Game, related_name='user_can_view')
    manager = models.ForeignKey(User, to_field='username', related_name='manager', on_delete=models.CASCADE, default='admin')
    def __str__(self):
        return str(self.user.username)
    @admin.display
    def game_admin(self):
        return "\n".join([(p.game_name + "\n") for p in self.game.all()])
    def get_queryset(self, request):
        if request.user.get_username() == "admin":
            return GameUser.objects.all()
        else:
            return GameUser.objects.filter(manager=request.user.get_username())

class Exchange(models.Model):
    date_update = models.DateField(primary_key=True)
    rate = models.JSONField()

class TotalGame(models.Model):
    date_update = models.DateField()
    product = models.ForeignKey(Game, to_field='id_bundle', on_delete=models.CASCADE)
    iaa = models.FloatField(default = 0)
    ecpm = models.FloatField(default = 0)
    impression = models.IntegerField(default = 0)
    iap = models.FloatField(default = 0)
    cost = models.FloatField(default = 0)
    cpi = models.FloatField(default = 0)
    install = models.FloatField(default = 0)
    class Meta:
        indexes = [
            models.Index(fields=['date_update']),
            models.Index(fields=['date_update', 'product'])
            ]
        managed = False
    def __str__(self):
        return self.product.name

class DetailGame(models.Model):
    date_update = models.DateField()
    product = models.ForeignKey(Game, to_field='id_bundle', on_delete=models.CASCADE)
    country = models.ForeignKey(Country, on_delete=models.CASCADE)
    network = models.CharField(max_length=255, default = 'Applovin')
    iaa = models.FloatField(default = 0)
    ecpm = models.FloatField(default = 0)
    impression = models.IntegerField(default = 0)
    iap = models.FloatField(default = 0)
    cost = models.FloatField(default = 0)
    cpi = models.FloatField(default = 0)
    install = models.FloatField(default = 0)
    class Meta:
        indexes = [
            models.Index(fields=['date_update']),
            models.Index(fields=['date_update', 'product', 'country', 'network'])
            ]
        managed = False
    def __str__(self):
        return self.product.name