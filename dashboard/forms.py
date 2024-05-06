"""
Definition of forms.
"""

from django import forms
from django.contrib.auth.forms import AuthenticationForm
from django.forms.widgets import NumberInput
from .models import Game, Country

class BootstrapAuthenticationForm(AuthenticationForm):
    """Authentication form which uses boostrap CSS."""
    username = forms.CharField(max_length=254,
                               widget=forms.TextInput({
                                   'class': 'form-control',
                                   'placeholder': 'User name'}))
    password = forms.CharField(label=("Password"),
                               widget=forms.PasswordInput({
                                   'class': 'form-control',
                                   'placeholder':'Password'}))

class DateForm(forms.Form):
    start_dt = forms.DateField(input_formats=['%d/%m/%Y'], widget=NumberInput(attrs={'type': 'date'}))
    end_dt = forms.DateField(input_formats=['%d/%m/%Y'], widget=NumberInput(attrs={'type': 'date'}))