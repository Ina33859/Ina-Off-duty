import math
import pandas as pd

def coordinate_transformation(org_coordinate:pd.DataFrame, transformation_list=[]):
    result_df = org_coordinate.copy()
    try:
        for transformation in transformation_list:
            transformation_type = transformation['type']
            x = result_df['x']
            y = result_df['y']
            if transformation_type == 'shift':
                x = result_df['x'] + transformation.get('shift_x', 0)
                y = result_df['y'] + transformation.get('shift_y', 0)
                pass
            elif transformation_type == 'rotate':
                direction = transformation.get('direction', '')
                angle = transformation.get('angle', 0)
                center = transformation.get('center', {'x':0,'y':0})
                if angle % 90 == 0:
                    if angle // 90 % 4 == (1 if direction == 'clockwise' else 3):
                        x = result_df['y'].map(lambda y:y)
                        y = result_df['x'].map(lambda x:-x)
                        pass
                    elif angle // 90 % 4 == 2:
                        x = result_df['x'].map(lambda x:-x)
                        y = result_df['y'].map(lambda y:-y)
                        pass
                    elif angle // 90 % 4 == (3 if direction == 'clockwise' else 1):
                        x = result_df['y'].map(lambda y:-y)
                        y = result_df['x'].map(lambda x:x)
                        pass
                    pass
                else:
                    if direction == 'clockwise':
                        x = result_df.apply(lambda row:(row['x']-center['x']) * round(math.cos(math.radians(angle)),2) + (row['y']-center['y']) * round(math.sin(math.radians(angle)),2),axis=1)
                        y = result_df.apply(lambda row:(row['y']-center['y']) * round(math.cos(math.radians(angle)),2) - (row['x']-center['x']) * round(math.sin(math.radians(angle)),2),axis=1)
                        pass
                    elif direction == 'counterclockwise':
                        x = result_df.apply(lambda row:(row['x']-center['x']) * round(math.cos(math.radians(angle)),2) - (row['y']-center['y']) * round(math.sin(math.radians(angle)),2),axis=1)
                        y = result_df.apply(lambda row:(row['x']-center['x']) * round(math.sin(math.radians(angle)),2) + (row['y']-center['y']) * round(math.cos(math.radians(angle)),2),axis=1)
                        pass
                    pass
                pass
            elif transformation_type == 'overturn':
                direction = transformation.get('direction', '')
                if direction == 'horizontal':
                    x = result_df['x'].map(lambda x:-x)
                    y = result_df['y'].map(lambda y:y)
                    pass
                elif direction == 'vertical':
                    x = result_df['x'].map(lambda x:x)
                    y = result_df['y'].map(lambda y:-y)
                    pass
                pass
            result_df['x'] = x
            result_df['y'] = y
            pass
        pass
    except Exception as ex:
        print('Coordinate transformate setting error.')
        result_df = org_coordinate.copy()
        pass
    finally:
        return result_df
