from django.contrib import admin
from django.contrib.auth.admin import UserAdmin
from django.core import serializers
from django.http import HttpResponse
from django.contrib.auth.models import User, Group
from sortedm2m_filter_horizontal_widget.forms import SortedFilteredSelectMultiple
admin.site.disable_action('delete_selected')
# Register your models here.
from .models import Game, Country, Network, GameUser
@admin.register(Game)
class GameAdmin(admin.ModelAdmin):
    actions = ['export_as_json']
    ordering=['name']
    list_display = ('name', 'id_bundle', 'creation_date')
    @admin.display(ordering='game__name')
    def admin_game_name(self, obj):
        return obj.game.name
    def get_queryset(self, request):
        return Game.get_queryset(self, request)
    def save_model(self, request, obj, form, change):
        super().save_model(request, obj, form, change)
        curUser = GameUser.objects.get(user = request.user)
        while (curUser.manager != curUser.user) and (curUser.user.get_username() != "admin"):
            obj.user_can_view.add(curUser)
            curUser = GameUser.objects.get(user = curUser.manager)
        super().save_model(request, obj, form, change)
    @admin.action(description='Export Selected Game')
    def export_as_json(modeladmin, request, queryset):
        response = HttpResponse(content_type="application/json")
        serializers.serialize("json", queryset, stream=response)
        return response
    search_fields = ['name', 'id_bundle']

class GameInline(admin.StackedInline):
    model = GameUser
    can_delete = False
    verbose_name_plural = 'Game User Manage'
    fk_name='user'
    fields = ['user', 'game']
    #filter_horizontal = ('games',)
    autocomplete_fields = ['user']
    search_fields = ['user']
    list_display = ('user', 'game_admin')
    search_fields = ['user']
    def save_model(self, request, obj, form, change):
        obj.manager = request.user.get_username()
        super().save_model(request, obj, form, change)
    def get_queryset(self, request):
        return GameUser.get_queryset(self, request)
    def formfield_for_manytomany(self, db_field, request, **kwargs):
        print(request.user.get_username())
        if db_field.name == "game":
            #kwargs["queryset"] = GameUser.objects.get(user_id = request.user.get_username()).game.all()
            if request.user.get_username() == "admin":
                kwargs["queryset"] = Game.objects.all()
            else:
                user = GameUser.objects.get(user = request.user.get_username())
                kwargs["queryset"] = Game.objects.filter(user_can_view=user)
            kwargs['widget'] = SortedFilteredSelectMultiple()
        return super().formfield_for_manytomany(db_field, request, **kwargs)

class FixedUserAdmin(UserAdmin):
    inlines = (GameInline,)
    def get_queryset(self, request):
        #return User.objects.filter(manager=request.user.get_username())
        query = GameUser.get_queryset(self, request)
        subquery = []
        for m in query:
            subquery.append(m.user)
        return User.objects.filter(username__in=subquery)
    def save_model(self, request, obj, form, change):
        super().save_model(request, obj, form, change)
        # try:
        #     game_user = GameUser.objects.get(user=request.user.get_username())
        # except GameUser.DoesNotExist:
        #     game_user = GameUser(user = User.objects.get(username=obj.username), manager = User.objects.get(username=request.user.get_username()))
        #     game_user.save()
        

admin.site.unregister(Group)
admin.site.unregister(User)
admin.site.register(User, FixedUserAdmin)
