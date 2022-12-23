import argparse
import datetime
import json
import pandas as pd
import numpy as np
    
def formatData(data: list) -> list:
    """change format into a more accessible one. 
    
    Args:
        data (list): successfull records containing spatio-temporal data of active tags
        
    Returns:
        list of lists: more accessible version of the original data format
        list: contains column names of respective data object
    """
    dataList = []
    columnNames = []
    for index, line in enumerate(data):
        newRow = []
        if("tagId" in line[0]):
            newRow.append(int(line[0]["tagId"]))
            if(index==0):
                columnNames.append("tagId")
        if("timestamp" in line[0]):
            newRow.extend((line[0]["timestamp"], line[0]["dateTime"]))
            if(index==0):
                columnNames.extend(("timestamp", "dateTime"))
        if("data" in line[0]):
            if("coordinates" in line[0]["data"]):
                newRow.extend((line[0]["data"]["coordinates"]["x"],
                              line[0]["data"]["coordinates"]["y"],
                              line[0]["data"]["coordinates"]["z"]))
                if(index==0):
                    columnNames.extend(("loc(x)", "loc(y)", "loc(z)"))
            if("orientation" in line[0]["data"]):
                newRow.extend((line[0]["data"]["orientation"]["yaw"],
                               line[0]["data"]["orientation"]["roll"],
                               line[0]["data"]["orientation"]["pitch"]))
                if(index==0):
                    columnNames.extend(("yaw", "roll", "pitch"))
            if("acceleration" in line[0]["data"]):
                newRow.extend((line[0]["data"]["acceleration"]["x"],
                               line[0]["data"]["acceleration"]["y"],
                               line[0]["data"]["acceleration"]["z"]))
                if(index==0):
                    columnNames.extend(("acc(x)", "acc(y)", "acc(z)"))
            if("tagData" in line[0]["data"]):
                if("gyro" in line[0]["data"]["tagData"]):
                    newRow.append(line[0]["data"]["tagData"]["gyro"])
                    if(index==0):
                        columnNames.append("gyro")
                if("magnetic" in line[0]["data"]["tagData"]):
                    newRow.append(line[0]["data"]["tagData"]["magnetic"])
                    if(index==0):
                        columnNames.append("magnetic")
                if("quaternion" in line[0]["data"]["tagData"]):
                    newRow.append(line[0]["data"]["tagData"]["quaternion"])
                    if(index==0):
                        columnNames.append("quaternion")
        dataList.append(newRow)
    return dataList, columnNames  
  
def timeConverter(successData: list) -> list:
    """Convert Epoch to more human-readable timestamp.
    Adapted from: https://docs.pozyx.io/enterprise/MQTT-data-structure.1224015817.html 
    
    Args:
        successData (list): successfull records from active tags
        
    Returns:
        original records with additional human-readable timestamp
    """
    for record in successData:
        record[0]["dateTime"] = datetime.datetime.fromtimestamp(record[0]["timestamp"]).strftime('%H:%M:%S,%f')
    return successData  

def analyseUnsuccessful(failureData: list, experiment_name: str) -> None:
    """Includes failure rate on a per tag basis. 
    
    Args:
        failureData (list): contains unsuccessfull active tag records
        experiment name (str): name of current experiment data
    
    Returns:
        nothing
    """
    all_tags, values = np.unique([tags[0]['tagId'] for tags in failureData], return_counts = True)
    total = sum(values)
    log_data = open(str(experiment_name) + "_logdata.txt", "a+")
    log_data.write("Failure statistics: ")
    for i in range(len(values)):
        log_data.write(f"Part rate of tag {all_tags[i]}: {round(values[i]/total*100)} %")
    log_data.close()   
    
def splitRecords(records: list, experiment_name: str) -> list:
    """Split between successful and unsuccessful records.
    
    Args:
        records (list): spatio-temporal data of active tags
        experiment_name (str): name of current experiment data
        
    Returns:
        list : containing succesfull records
        list : containing unsuccesfull records
    """    
    arrSuccess, arrFailure = [], []
    updateRate = []
    log_data = open(str(experiment_name) + "_logdata.txt", "w")
    for record in records:
        json_record = json.loads(record)
        if(json_record[0]["success"]==False):
            arrFailure.append(json_record)
        else:
            arrSuccess.append(json_record)
            updateRate.append([json_record[0]["tagId"],json_record[0]["data"]["metrics"]["rates"]["update"]])
    success, failure = len(arrSuccess), len(arrFailure)
    total = len(arrSuccess)+len(arrFailure)
    concatInf = (f"Number of successful measurements: {success}\n"
                  f"Number of unsuccessful measurements: {failure}\n"
                  f"Total number of measurements: {total}\n"
                  f"Success rate: {round(success/total*100,2)} %\n"
                  f"Failure rate: {round(failure/total*100,2)} %\n")
    log_data.write(concatInf) # new line
    df = pd.DataFrame(updateRate, columns = ["tag", "rate"])
    log_data.write(str(df.groupby("tag", as_index = False)["rate"].mean())+ "\n")
    log_data.close()

    return arrFailure, arrSuccess

def clear_lines(lines: list)-> list:
    """Filters lines from unnecessary phrases
    
    Args:
        lines (list): contain active tag information
    
    Returns:
        list:  filtered lines
    """
    cleared_lines = []
    phrase_1 = 'Connection Accepted.\n'
    phrase_2 = 'Subscribed to topic!\n'
    phrase_3 = 'Positioning update: '

    for line in lines:
        if (line == phrase_1)|(line == phrase_2): 
            continue
        else:
            line = line.replace(phrase_3, "")
            cleared_lines.append(line)
    return cleared_lines

def read_file(path: str)-> list:
    """It reads the file in path directory
    
    Args:
        path (str) : path directory to file
    
    Returns:
        list: lines of the file in question
    
    """
    with open(path, 'r') as f:
        lines = f.readlines()
    f.close()
    return lines        
 
def main():
    parser = argparse.ArgumentParser()

    parser.add_argument('--inputfile', type=str, required='text1.txt')
    parser.add_argument('--experiment', type=str, default='test')

    args = parser.parse_args()
    
    # open the formatted raw data file.
    path = args.inputfile
    experiment_name = args.experiment
    lines = read_file(path)
    
    # filter out data from junk information
    cleared_lines = clear_lines(lines)
    #split between successful and unsuccessful records
    failureData, successData = splitRecords(cleared_lines, experiment_name)
    #check the unscussessful measurements
    analyseUnsuccessful(failureData, experiment_name)
    #converter to more human readable time
    successData = timeConverter(successData)
    #change the format of the data
    formattedData, columnNames = formatData(successData)
    #create a pandas DataFrame
    df = pd.DataFrame(data = formattedData, columns = columnNames)
    
    
    df.to_csv(str(experiment_name) + "_df.csv", index = False)

    
if __name__ == "__main__":
    main()
