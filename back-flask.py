import pymysql
from flask import Flask, jsonify, request
from flask_cors import CORS
from shapely import wkt
from shapely.errors import GEOSException
from shapely.geometry import Polygon
from shapely.wkt import loads

app = Flask(__name__)
CORS(app)

# MySQL 数据库配置
db_config = {
    'host': '192.168.0.14',
    'user': 'root',
    'password': '0601',
    'database': 'yaogan',
    'cursorclass': pymysql.cursors.DictCursor
}


# 定义一个根路径的路由
@app.route('/')
def home():
    return "Welcome to the Flask Application!"


# def query_coordinates_by_date(date):
#     try:
#         connection = pymysql.connect(**db_config)
#         cursor = connection.cursor()
#
#         # 查询指定日期的记录
#         query = "SELECT ID, Date, Area, ST_AsText(Location) AS Location FROM greenland WHERE Date = %s"
#         cursor.execute(query, (date,))
#
#         results = cursor.fetchall()
#
#         parsed_results = []
#         for row in results:
#             try:
#                 # 将Location字段转换为坐标
#                 location_polygon = loads(row['Location'])
#                 parsed_results.append({
#                     'ID': row['ID'],
#                     'Date': row['Date'],
#                     'Area': row['Area'],
#                     'Coordinates': list(location_polygon.exterior.coords)
#                 })
#             except GEOSException as ge:
#                 print(f"Invalid WKT format for ID {row['ID']}: {ge}")
#
#         cursor.close()
#         connection.close()
#
#         return parsed_results
#     except pymysql.MySQLError as e:
#         print("MySQL Error:", e)
#         return {"error": "Database query failed"}
#     except Exception as e:
#         print("General Error:", e)
#         return {"error": f"An error occurred: {e}"}
def query_coordinates_by_date(date):
    try:
        connection = pymysql.connect(**db_config)
        cursor = connection.cursor()

        # 查询greenland6表中指定日期的记录
        query = """
            SELECT 
                ID, 
                Date, 
                Area, 
                ST_AsText(Location) AS Location_WKT, 
                ST_AsText(Center) AS Center_WKT, 
                Ratios,
                Trans  -- Trans 是面积变化
            FROM greenland6
            WHERE Date = %s
        """
        cursor.execute(query, (date,))

        results = cursor.fetchall()

        parsed_results = []
        max_area_increase = float('-inf')  # 最大面积增加值
        min_area_decrease = float('inf')  # 最大面积减少值
        max_increase_record = None
        max_decrease_record = None

        # 解析查询结果
        for row in results:
            try:
                # 将Location字段转换为Shapely对象
                location_polygon = wkt.loads(row['Location_WKT'])
                parsed_results.append({
                    'ID': row['ID'],
                    'Date': row['Date'],
                    'Area': row['Area'],
                    'Coordinates': list(location_polygon.exterior.coords),
                    'Change': row['Trans'],  # 使用Trans作为面积变化
                    'Center': row['Center_WKT'],
                    'Ratios': row['Ratios']
                })

                # 计算面积变化最大和最小的多边形
                if row['Trans'] > max_area_increase:
                    max_area_increase = row['Trans']
                    max_increase_record = {
                        'ID': row['ID'],
                        'Date': row['Date'],
                        'Area': row['Area'],
                        'Coordinates': list(location_polygon.exterior.coords),
                        'Change': row['Trans'],
                        'Center': row['Center_WKT'],
                        'Ratios': row['Ratios']
                    }

                if row['Trans'] < min_area_decrease:
                    min_area_decrease = row['Trans']
                    max_decrease_record = {
                        'ID': row['ID'],
                        'Date': row['Date'],
                        'Area': row['Area'],
                        'Coordinates': list(location_polygon.exterior.coords),
                        'Change': row['Trans'],
                        'Center': row['Center_WKT'],
                        'Ratios': row['Ratios']
                    }

            except GEOSException as ge:
                print(f"Invalid WKT format for ID {row['ID']}: {ge}")

        cursor.close()
        connection.close()

        # 返回所有记录并添加面积变化最大和最小的记录
        result = {
            'all_polygons': parsed_results,
            'max_increase_record': max_increase_record,
            'max_decrease_record': max_decrease_record
        }

        return result

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
    app.run(host='0.0.0.0', debug=True)
