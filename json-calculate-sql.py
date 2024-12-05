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


def extract_geo_bounds(json_data):
    """
    从 JSON 数据中提取地理边界（四个角点坐标）。
    """
    # 提取四个角点的经纬度
    topleft_lat = float(json_data["topleftlatitude"])
    topleft_lon = float(json_data["topleftlongitude"])
    bottomright_lat = float(json_data["bottomrightlatitude"])
    bottomright_lon = float(json_data["bottomrightlongitude"])

    # 构建地理多边形（Shapely Polygon）
    coords = [
        (topleft_lon, topleft_lat),  # 左上角
        (bottomright_lon, topleft_lat),  # 右上角
        (bottomright_lon, bottomright_lat),  # 右下角
        (topleft_lon, bottomright_lat),  # 左下角
        (topleft_lon, topleft_lat)  # 闭合多边形
    ]
    print(Polygon(coords))
    return Polygon(coords)


def pixel_to_geo(polygon, img_width, img_height, geo_bounds):
    """
    将像素坐标转换为地理坐标。
    """
    min_lon, min_lat, max_lon, max_lat = geo_bounds.bounds
    lon_per_pixel = (max_lon - min_lon) / img_width
    lat_per_pixel = (max_lat - min_lat) / img_height

    geo_coords = [
        (
            min_lon + point[0] * lon_per_pixel,
            max_lat - point[1] * lat_per_pixel  # Y 轴需要反转
        )
        for point in np.array(polygon.exterior.coords)
    ]
    return Polygon(geo_coords)


def process_files_to_db(json_path, png_path, shenzhen_shp_path, cursor, record_id):
    """
    处理单个 JSON 文件和 PNG 文件，并将结果插入到数据库。
    """
    try:
        # 加载 JSON 文件
        with open(json_path, 'r') as f:
            json_data = json.load(f)
        print(f"加载 JSON 数据成功: {json_path}")

        # 提取地理边界
        geo_bounds = extract_geo_bounds(json_data)

        # 加载 PNG 文件
        image = cv2.imread(png_path, cv2.IMREAD_GRAYSCALE)
        if image is None:
            print(f"无法加载 PNG 文件: {png_path}")
            return record_id
        print(f"加载 PNG 文件成功，尺寸: {image.shape}")

        img_height, img_width = image.shape

        # Step 1: 二值化处理
        _, binary_image = cv2.threshold(image, 127, 255, cv2.THRESH_BINARY)

        # Step 2: 去噪（高斯模糊）
        binary_image = cv2.GaussianBlur(binary_image, (5, 5), 0)

        # Step 3: 提取轮廓
        contours, _ = cv2.findContours(binary_image, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)

        # Step 4: 优化轮廓并验证有效性
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

        # Step 5: 转换为地理坐标
        geo_polygons = [pixel_to_geo(polygon, img_width, img_height, geo_bounds) for polygon in polygons]

        # 加载深圳边界 Shapefile
        shenzhen_boundary = gpd.read_file(shenzhen_shp_path).geometry.unary_union

        # 筛选在深圳边界内的多边形
        filtered_polygons = [polygon for polygon in geo_polygons if polygon.within(shenzhen_boundary)]
        print(f"筛选后多边形数量: {len(filtered_polygons)}")

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
            INSERT INTO greenland2 (ID, Date, Area, Location)
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


def batch_process(metadata_dir, png_dir, shenzhen_shp_path, connection):
    """
    批量处理 JSON 和 PNG 文件，并将结果插入到数据库。
    """
    try:
        # 读取 metadata_dir 目录下所有以 .json 结尾的文件，形成 JSON 文件列表
        json_files = [f for f in os.listdir(metadata_dir) if f.endswith(".json")]

        # 读取 png_dir 目录下所有以 .png 结尾的文件，形成 PNG 文件列表
        png_files = [f for f in os.listdir(png_dir) if f.endswith(".png")]

        # 建立 JSON 文件名到路径的映射
        json_map = {
            f.split('-')[-1].replace('.json', ''): os.path.join(metadata_dir, f) for f in json_files
        }

        # 建立 PNG 文件名到路径的映射
        png_map = {
            f.split('_')[-1].split('L1A')[-1].replace('.png', '').lstrip('0'): os.path.join(png_dir, f) for f in png_files
        }

        print(f"匹配到的 JSON 文件数量: {len(json_map)}")
        print(f"匹配到的 PNG 文件数量: {len(png_map)}")

        record_id = 1
        with connection.cursor() as cursor:
            for key in png_map.keys():
                if key in json_map:
                    json_path = json_map[key]
                    png_path = png_map[key]
                    print(f"匹配成功: JSON={json_path}, PNG={png_path}")
                    record_id = process_files_to_db(json_path, png_path, shenzhen_shp_path, cursor, record_id)
                else:
                    print(f"未找到匹配的 JSON 文件: PNG={png_map[key]}")

            connection.commit()  # 提交事务
    except Exception as e:
        print(f"批量处理时出错: {e}")


# 配置路径
metadata_dir = r"D:\Project\yaogan\metadata"  # JSON 文件目录
png_dir = r"D:\Project\yaogan\png"  # PNG 文件目录
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
batch_process(metadata_dir, png_dir, shenzhen_shp, connection)

# 关闭数据库连接
connection.close()
