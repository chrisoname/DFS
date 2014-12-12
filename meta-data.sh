cd node1
rm *
cd ../node2
rm *
cd ..
rm *.pyc
rm dfs.db
python createdb.py
clear
echo "starting server"
python meta-data.py
