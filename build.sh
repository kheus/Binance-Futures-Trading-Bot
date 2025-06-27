#!/bin/bash

# Met à jour les paquets et installe les dépendances système
apt-get update && apt-get install -y build-essential wget

# Installe TA-Lib (librairie C)
wget http://prdownloads.sourceforge.net/ta-lib/ta-lib-0.4.0-src.tar.gz
tar -xvzf ta-lib-0.4.0-src.tar.gz
cd ta-lib-0.4.0
./configure --prefix=/usr
make
make install
cd ..
rm -rf ta-lib-0.4.0*

# Installe les dépendances Python
pip install --upgrade pip
pip install -r src/requirements.txt

