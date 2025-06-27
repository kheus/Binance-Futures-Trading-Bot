#!/bin/bash

# Met à jour les paquets et installe les dépendances système
apt-get update && apt-get install -y build-essential wget

# Installe TA-Lib (librairie C)
pip install ta-lib==0.4.19

# Installe les dépendances Python
pip install --upgrade pip
pip install -r src/requirements.txt

