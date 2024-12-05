import os
import cv2
import json
import numpy as np
import pymysql
from shapely.geometry import Polygon
from shapely.ops import transform
import geopandas as gpd
from datetime import datetime
import pyproj


def parse_jgw(jgw_path):
    """
    解析 .jgw 文件，返回地理参考参数。
    """
    with open(jgw_path, 'r') as f:
        lines = f.readlines()

    if len(lines) != 6:
        raise ValueError(f"无效的 .jgw 文件: {jgw_path}，必须包含 6 行数据")

    values = [float(line.strip()) for line in lines]
    return {
        "pixel_width": values[0] / 10,  # 每像素宽度
        "rotation_x": values[1] / 10,  # 行旋转
        "rotation_y": values[2] / 10,  # 列旋转
        "pixel_height": values[3] / 10,  # 每像素高度（通常为负）
        "top_left_x": values[4],  # 左上角 X 坐标
        "top_left_y": values[5]  # 左上角 Y 坐标
    }


def pixel_to_geo_with_jgw(polygon, jgw_params):
    """
    根据 .jgw 文件参数，将像素坐标转换为地理坐标。
    """
    A = jgw_params["pixel_width"]
    B = jgw_params["rotation_x"]
    D = jgw_params["rotation_y"]
    E = jgw_params["pixel_height"]
    C = jgw_params["top_left_x"]
    F = jgw_params["top_left_y"]

    geo_coords = [
        (
            A * point[0] + B * point[1] + C,  # x
            D * point[0] + E * point[1] + F   # y
        )
        for point in np.array(polygon.exterior.coords)
    ]
    return Polygon(geo_coords)


def process_files_to_db(json_path, png_path, jgw_path, shenzhen_shp_path, cursor, record_id):
    """
    处理单个 JSON 文件和 PNG 文件，并将结果保存到 MySQL 数据库。
    """
    try:
        # 加载 JSON 文件
        with open(json_path, 'r') as f:
            json_data = json.load(f)
        print(f"加载 JSON 数据成功: {json_path}")

        # 加载 JGW 文件参数
        jgw_params = parse_jgw(jgw_path)

        # 加载 PNG 文件
        image = cv2.imread(png_path, cv2.IMREAD_GRAYSCALE)
        if image is None:
            print(f"无法加载 PNG 文件: {png_path}")
            return record_id
        print(f"加载 PNG 文件成功，尺寸: {image.shape}")

        # 二值化处理
        _, binary_image = cv2.threshold(image, 127, 255, cv2.THRESH_BINARY)

        # 提取轮廓
        contours, _ = cv2.findContours(binary_image, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)

        # 筛选有效的多边形
        polygons = []
        for contour in contours:
            # 使用 approxPolyDP 简化轮廓，减少冗余顶点
            epsilon = 0.0025 * cv2.arcLength(contour, True)
            simplified_contour = cv2.approxPolyDP(contour, epsilon, True)

            # 确保轮廓至少有 3 个顶点（有效多边形）
            if len(simplified_contour) >= 3:
                # 转换为 Shapely 多边形
                polygon = Polygon(simplified_contour[:, 0, :])
                if polygon.is_valid:  # 确保多边形有效
                    polygons.append(polygon)

        print(f"优化后有效多边形数量: {len(polygons)}")

        # 转换为地理坐标（使用 JGW 文件参数）
        geo_polygons = [pixel_to_geo_with_jgw(polygon, jgw_params) for polygon in polygons]

        # 加载深圳边界 Shapefile
        shenzhen_boundary = gpd.read_file(shenzhen_shp_path).geometry.unary_union

        # 筛选在深圳边界内的多边形
        filtered_polygons = [polygon for polygon in geo_polygons if polygon.within(shenzhen_boundary)]
        print(f"筛选后多边形数量: {len(filtered_polygons)}")

        # 去除重复的多边形
        filtered_polygons = list(set(filtered_polygons))

        # 定义 EPSG:4326 到 EPSG:3857 的转换器
        project_to_3857 = pyproj.Transformer.from_crs("EPSG:4326", "EPSG:3857", always_xy=True).transform

        # 提取 JSON 中的日期
        start_date = datetime.strptime(json_data["starttime"], "%Y-%m-%d %H:%M:%S").strftime("%Y-%m-%d")

        # 插入数据到数据库
        for polygon in filtered_polygons:
            # 转换为 EPSG:3857（以米为单位）计算面积
            metric_polygon = transform(project_to_3857, polygon)
            area = metric_polygon.area  # 面积单位：平方米

            # 保留原始 EPSG:4326 的多边形顶点
            # 调整顶点坐标为 (纬度, 经度) 顺序
            lat_lon_polygon = Polygon([(y, x) for x, y in polygon.exterior.coords])
            location_wkt = lat_lon_polygon.wkt  # 使用调整后的多边形 WKT 表示

            sql = """
                    INSERT INTO greenland3 (ID, Date, Area, Location)
                    VALUES (%s, %s, %s, ST_GeomFromText(%s, 4326))
                    """
            try:
                cursor.execute(sql, (record_id, start_date, area, location_wkt))
                record_id += 1
                print(f"成功插入记录: ID={record_id - 1}, Area={area}, Date={start_date}")
            except pymysql.MySQLError as e:
                print(f"插入数据时出错: {e}")

    except Exception as e:
        print(f"处理文件时出错: JSON={json_path}, PNG={png_path}, 错误: {e}")

    return record_id


def batch_process(metadata_dir, png_dir, jgw_dir, shenzhen_shp_path, connection):
    """
    批量处理 JSON、PNG 和 JGW 文件，并将结果保存到 MySQL 数据库。
    """
    try:
        # 读取 metadata_dir 目录下所有以 .json 结尾的文件，形成 JSON 文件列表
        json_files = [f for f in os.listdir(metadata_dir) if f.endswith(".json")]

        # 读取 png_dir 目录下所有以 .png 结尾的文件，形成 PNG 文件列表
        png_files = [f for f in os.listdir(png_dir) if f.endswith(".png")]

        # 读取 jgw_dir 目录下所有以 .jgw 结尾的文件，形成 JGW 文件列表
        jgw_files = [f for f in os.listdir(jgw_dir) if f.endswith(".jgw")]

        # 建立 JSON 文件名到路径的映射
        json_map = {
            f.split('-')[-1].replace('.json', ''): os.path.join(metadata_dir, f) for f in json_files
        }

        # 建立 PNG 文件名到路径的映射
        png_map = {
            f.split('_')[-1].split('L1A')[-1].replace('.png', '').lstrip('0'): os.path.join(png_dir, f) for f in png_files
        }

        # 建立 JGW 文件名到路径的映射
        jgw_map = {
            f.split('-')[-1].replace('.jgw', ''): os.path.join(jgw_dir, f) for f in jgw_files
        }

        print(f"匹配到的 JSON 文件数量: {len(json_map)}")
        print(f"匹配到的 PNG 文件数量: {len(png_map)}")
        print(f"匹配到的 JGW 文件数量: {len(jgw_map)}")

        record_id = 1
        with connection.cursor() as cursor:
            for key in png_map.keys():
                if key in json_map and key in jgw_map:
                    json_path = json_map[key]
                    png_path = png_map[key]
                    jgw_path = jgw_map[key]
                    print(f"匹配成功: JSON={json_path}, PNG={png_path}, JGW={jgw_path}")
                    record_id = process_files_to_db(json_path, png_path, jgw_path, shenzhen_shp_path, cursor, record_id)
                else:
                    print(f"未找到匹配的文件: Key={key}")

            connection.commit()  # 提交事务
    except Exception as e:
        print(f"批量处理时出错: {e}")



# 配置路径
metadata_dir = r"D:\Project\yaogan\metadata-jgw"  # JSON 文件目录
png_dir = r"D:\Project\yaogan\png"  # PNG 文件目录
jgw_dir = r"D:\Project\yaogan\metadata-jgw"  # JGW 文件目录
shenzhen_shp = r"D:\Project\yaogan\深圳市边界_440300_Shapefile_(poi86.com)\440300.shp"  # 深圳边界 Shapefile 文件路径

# 数据库配置
connection = pymysql.connect(
    host='192.168.0.14',
    user='root',
    password='0601',
    database='yaogan',
    charset='utf8mb4'
)

# 批量处理
batch_process(metadata_dir, png_dir, jgw_dir, shenzhen_shp, connection)

# 关闭数据库连接
connection.close()
