rm *.pyc
rm dfs.db
python createdb.py
echo "starting server"
python meta-data.py
echo "listening"
