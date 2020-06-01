#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import mysql.connector
from mysql.connector import Error
import cv2
import numpy as np
from io import BytesIO 
from io import StringIO
import PIL.Image


# In[ ]:


# id_photo = 1
path = "E:/to_mysql/1.jpg"
det = 0
ndet = 0
detFlag = 0
# total_data = total data di database + 1
total_data = 57031

face_cascade = cv2.CascadeClassifier('haarcascade_frontalface_default.xml')

def write_file(data, filename):
    with open(filename, 'wb') as file:
        file.write(data)

def readBLOB(id_photo, path):
    print("Reading BLOB data from photo_raw table")

    try:
        connection = mysql.connector.connect(host='localhost',
                                             database='smavi',
                                             user='root',
                                             password='isi_password',
                                             use_pure=True)

        cursor = connection.cursor()
        sql_fetch_blob_query = "SELECT * from photo_raw where id = %s"

        cursor.execute(sql_fetch_blob_query, (id_photo,))
        record = cursor.fetchall()
        for row in record:
            print("Id = ", row[0], )
            image = row[1]
            print("Storing image on disk")
            write_file(image, path)
        

    except mysql.connector.Error as error:
        print("Failed to read BLOB data from MySQL table {}".format(error))

    finally:
        if (connection.is_connected()):
            cursor.close()
            connection.close()
            print("MySQL connection is closed --- readBLOB")
            
def readIMG(id_photo, path, det, ndet, detFlag):
    img = cv2.imread(path,cv2.IMREAD_COLOR)
    gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
    faces = face_cascade.detectMultiScale(gray, 1.1, 1)
    # print(type(faces))
    
    try:
        if faces.size:
            print("detected")
            det += 1
            detFlag = 1
            # print("dalam", det, ndet, detFlag)
    except:
        print("not detected")
        ndet += 1
        detFlag = 2
        # print("dalam", det, ndet, detFlag)
        
    for (x, y, w, h) in faces:
        cv2.rectangle(img, (x, y), (x+w, y+h), (255, 0, 0), 2)
    # cv2.imshow('image',img)
    # cv2.waitKey(0)
    # cv2.destroyAllWindows()
    
    return det, ndet, detFlag

def convertToBinaryData(filename):
    # Convert digital data to binary format
    with open(filename, 'rb') as file:
        binaryData = file.read()
    return binaryData

def insertBLOB_detected(id_photo, photo):
    print("Inserting BLOB into smavi faces database")
    try:
        connection = mysql.connector.connect(host='localhost',
                                             database='smavi',
                                             user='root',
                                             password='isi_password')

        cursor = connection.cursor()
        sql_insert_blob_query = "INSERT INTO faces (id, photo) VALUES (%s,%s)"

        empPicture = convertToBinaryData(photo)

        # Convert data into tuple format
        insert_blob_tuple = (id_photo, empPicture)
        result = cursor.execute(sql_insert_blob_query, insert_blob_tuple)
        connection.commit()
        print("Image inserted successfully as a BLOB into smavi faces database", result)

    except mysql.connector.Error as error:
        print("Failed inserting BLOB data into MySQL table {}".format(error))

    finally:
        if (connection.is_connected()):
            cursor.close()
            connection.close()
            print("MySQL connection is closed --- insertDetect \n")

def insertBLOB_not_detected(id_photo, photo):
    print("Inserting BLOB into smavi no_faces database")
    try:
        connection = mysql.connector.connect(host='localhost',
                                             database='smavi',
                                             user='root',
                                             password='isi_password')

        cursor = connection.cursor()
        sql_insert_blob_query = "INSERT INTO no_faces (id, photo) VALUES (%s,%s)"

        empPicture = convertToBinaryData(photo)

        # Convert data into tuple format
        insert_blob_tuple = (id_photo, empPicture)
        result = cursor.execute(sql_insert_blob_query, insert_blob_tuple)
        connection.commit()
        print("Image inserted successfully as a BLOB into smavi no_faces database", result)

    except mysql.connector.Error as error:
        print("Failed inserting BLOB data into MySQL table {}".format(error))

    finally:
        if (connection.is_connected()):
            cursor.close()
            connection.close()
            print("MySQL connection is closed --- insertNoDetect \n")
            

for idd in range(1,total_data):
    readBLOB(idd, path)
    det, ndet, detFlag = readIMG(idd, path, det, ndet, detFlag)
    # print(det, ndet, detFlag)
    if detFlag == 1:
        insertBLOB_detected(idd, path)
    elif detFlag == 2:
        insertBLOB_not_detected(idd, path)

