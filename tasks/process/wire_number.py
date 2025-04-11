import os
import pandas as pd
from tasks.process import process
from utils.connect import connect_data

class wire_number_process(process):
    def __init__(self, connect: connect_data) -> None:
        super().__init__(connect.duckdb, connect.mongo, "线号标签申请数据处理", "wire_number_process")
        self.path = "\\\\172.18.1.60\\部室文件(新)\\总成车间（新）\\4.工位工作\\线号标签申请\\"
        
    def task_main(self) -> None:
        columns=[
            "4.8黄 上标",
            "4.8黄 下标",
            "4.8白 上标",
            "4.8白 下标",
            "6.4黄 上标",
            "6.4黄 下标",
            "6.4白 上标",
            "6.4白 下标",
            "6.4红 上标",
            "6.4红 下标",
            "9.5黄 上标",
            "9.5黄 下标",
            "9.5白 上标",
            "9.5白 下标",
            "9.5红 上标",
            "9.5红 下标",
            "12.7黄 上标",
            "12.7黄 下标",
            "12.7白 上标",
            "12.7白 下标",
            "12.7红 上标",
            "12.7红 下标",
            "19.0黄 上标",
            "19.0黄 下标",
            "19.0白 上标",
            "19.0白 下标",
            "19.0红 上标",
            "19.0红 下标",
            "25.4白 上标",
            "25.4白 下标",
            "25.4红 上标",
            "25.4红 下标"
        ]  
        temp={
            "4.8黄":{},
            "4.8白":{},
            "6.4黄":{},
            "6.4白":{},
            "6.4红":{},
            "9.5黄":{},
            "9.5白":{},
            "9.5红":{},
            "12.7黄":{},
            "12.7白":{},
            "12.7红":{},
            "19.0黄":{},
            "19.0白":{},
            "19.0红":{},
            "25.4白":{},
            "25.4红":{}
        } 
        df_main: pd.DataFrame = self.connect.sql(f"SELECT * FROM ods.wire_number_head").fetchdf()
        df_main_e1: pd.DataFrame = self.connect.sql(f"SELECT * FROM ods.wire_number_entry").fetchdf()
        for _, row in df_main.iterrows():
            if f"""{str(row["项目名称"])} {str(row["创建人"])}""" not in temp[str(row["规格型号"])].keys():
                temp[str(row["规格型号"])][f"""{str(row["项目名称"])} {str(row["创建人"])}"""] = []
            temp[str(row["规格型号"])][f"""{str(row["项目名称"])} {str(row["创建人"])}"""].append([str(row["上标"]), str(row["下标"])])
        ttt = []
        for col_name in columns:
            temp_col = [col_name]
            guige = col_name.split(" ")[0]
            up_or_down = col_name.split(" ")[1]
            for name in temp[guige].keys():
                project = name.split(" ")[0]
                person_name = name.split(" ")[1]
                if up_or_down == "上标":
                    temp_col.append(project)
                    for value in temp[guige][name]:
                        temp_col.append(value[0])
                elif up_or_down == "下标":
                    temp_col.append(person_name)
                    for value in temp[guige][name]:
                        temp_col.append(value[1])
                else:
                    raise ValueError()
            ttt.append(temp_col)
        pd.DataFrame(ttt).T.to_excel(os.path.join(self.path, "temp1.xlsx"), sheet_name="sheet1", header=False, index=False)
        
        temp1: dict[str, list] = {}
        for _, rows in df_main_e1.iterrows():
            if rows["创建人"] not in temp1.keys():
                temp1[rows["创建人"]] = []
            temp1[rows["创建人"]].append([rows["位置号"], rows["连接器代号"]])
        
        ppp = []
        for col_name in temp1.keys():
            temp_list_0 = []
            temp_list_1 = []
            temp_list_0.append(col_name)
            temp_list_1.append("")
            for ch in temp1[col_name]:
                temp_list_0.append(ch[0])
                temp_list_1.append(ch[1])
            ppp.append(temp_list_0)  
            ppp.append(temp_list_1)
        pd.DataFrame(ppp).T.to_excel(os.path.join(self.path, "temp2.xlsx"), sheet_name="sheet1", header=False, index=False) 