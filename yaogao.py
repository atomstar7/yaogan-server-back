from flask import Flask, jsonify, request
import pymysql
from shapely.geometry import Polygon, shape
from shapely.wkt import loads
from shapely.errors import GEOSException

app = Flask(__name__)

# MySQL 数据库配置
db_config = {
    'host': 'localhost',
    'user': 'root',
    'password': '0601',
    'database': 'yaogan',
    'cursorclass': pymysql.cursors.DictCursor
}

# 定义一个根路径的路由
@app.route('/')
def home():
    return "Welcome to the Flask Application!"

def query_coordinates_by_date(date):
    try:
        connection = pymysql.connect(**db_config)
        cursor = connection.cursor()
        
        # 查询指定日期的记录
        query = "SELECT ID, Date, Area, ST_AsText(Location) AS Location FROM greenland WHERE Date = %s"
        cursor.execute(query, (date,))
        
        results = cursor.fetchall()
        
        parsed_results = []
        for row in results:
            try:
                # 将Location字段转换为坐标
                location_polygon = loads(row['Location'])
                parsed_results.append({
                    'ID': row['ID'],
                    'Date': row['Date'],
                    'Area': row['Area'],
                    'Coordinates': list(location_polygon.exterior.coords)
                })
            except GEOSException as ge:
                print(f"Invalid WKT format for ID {row['ID']}: {ge}")

        cursor.close()
        connection.close()
        
        return parsed_results
    except pymysql.MySQLError as e:
        print("MySQL Error:", e)
        return {"error": "Database query failed"}
    except Exception as e:
        print("General Error:", e)
        return {"error": f"An error occurred: {e}"}

@app.route('/api/coordinates', methods=['GET'])
def get_coordinates():
    date = request.args.get('Date')
    if not date:
        return jsonify({"error": "Missing required parameter: Date"}), 400
    
    data = query_coordinates_by_date(date)
    if "error" in data:
        return jsonify(data), 500
    
    return jsonify(data)

def query_coordinates_in_area(area_coords):
    try:
        connection = pymysql.connect(**db_config)
        cursor = connection.cursor()
        
        # 查询所有记录
        query = "SELECT ID, Date, Area, ST_AsText(Location) AS Location FROM greenland"
        cursor.execute(query)
        
        results = cursor.fetchall()
        
        # 构造查询区域的 Polygon 对象
        query_polygon = Polygon(area_coords)
        
        filtered_results = []
        for row in results:
            try:
                location_polygon = loads(row['Location'])
                if location_polygon.intersects(query_polygon):
                    filtered_results.append({
                        'ID': row['ID'],
                        'Date': row['Date'],
                        'Area': row['Area'],
                        'Coordinates': list(location_polygon.exterior.coords)
                    })
            except GEOSException as ge:
                print(f"Invalid WKT format for ID {row['ID']}: {ge}")

        cursor.close()
        connection.close()
        
        return filtered_results
    except pymysql.MySQLError as e:
        print("MySQL Error:", e)
        return {"error": "Database query failed"}
    except Exception as e:
        print("General Error:", e)
        return {"error": f"An error occurred: {e}"}

@app.route('/api/coordinates_in_area', methods=['GET'])
def get_coordinates_in_area():
    area = request.args.get('Area')
    if not area:
        return jsonify({"error": "Missing required parameter: Area"}), 400
    
    try:
        area_coords = eval(area)
    except:
        return jsonify({"error": "Invalid Area format. It should be an array of coordinates."}), 400
    
    data = query_coordinates_in_area(area_coords)
    if "error" in data:
        return jsonify(data), 500
    
    return jsonify(data)

# 启动 Flask 应用
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=6666, debug=True)
