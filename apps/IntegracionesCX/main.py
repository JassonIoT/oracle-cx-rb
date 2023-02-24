from IntegracionesCX.connection.connection import Connection
from IntegracionesCX.base.base import Base
#from fastapi import FastAPI, File, UploadFile, Form
#from fastapi.responses import StreamingResponse, FileResponse, HTMLResponse
from random import randint
import uuid
import pandas as pd
from datetime import date, datetime, timedelta
import threading




def operacionUno(container):
    try:
        print('========================================================')
        print('Entra a operacion uno')
        print(container)
    except Exception as e:
        print(e)
        traceback.print_exc()
    print('==========================================================')


def operacionDos():
    try:
        print('**********************************************************')
        print('Entra operacionDos')
    except Exception as e:
        print('error en operacionDos')
        print(e)
        traceback.print_exc()
    print('************************************************************')


def testdb_db():
    print('Inicia test')
    c = Connection()
    ver = c.test()
    return ver

def conectionTest(namedb):
    print(namedb)
    name_db= namedb.name_db
    print(name_db)
    print(type(name_db))
    t = threading.Thread(target=run_base, args=(name_db,))
    t.start()
    return 'Peticion Ok'
"""
@app.post("/upload_img")
def image_filter(img: UploadFile = File(...)):
    original_image = Image.open(img.file)
    original_image = original_image.filter(ImageFilter.BLUR)

    filtered_image = BytesIO()
    original_image.save(filtered_image, "JPEG")
    filtered_image.seek(0)

    return StreamingResponse(filtered_image, media_type="image/jpeg")


@app.get("/download_img/{image_id}", response_class=FileResponse)
async def get_image(image_id):

    file_path = f"{IMAGEDIR}{image_id}"
    return FileResponse(file_path)


@app.post("/images/")
async def create_upload_file(file: UploadFile = File(...)):

    file.filename = f"{uuid.uuid4()}.jpg"
    contents = await file.read()  # <-- Important!

    # example of how you can save the file
    with open(f"{IMAGEDIR}{file.filename}", "wb") as f:
        f.write(contents)

    return {"filename": file.filename}


@app.get("/images/")
async def read_random_file():

    # get a random file from the image directory
    files = os.listdir(IMAGEDIR)
    random_index = randint(0, len(files) - 1)

    path = f"{IMAGEDIR}{files[random_index]}"
    
    # notice you can use FileResponse now because it expects a path
    return FileResponse(path)


@app.get("/xlsfile", response_description='xlsx')
async def xlsx_file():
    frame = pd.DataFrame([[11, 21, 31], [12, 22, 32], [31, 32, 33]],
                  index=['one', 'two', 'three'], columns=['a', 'b', 'c'])

    # frame = pd.read_excel("filename.xlsx")
    output = BytesIO()

    # frame.to_excel('example.xlsx', sheet_name='new_sheet_name')

    with pd.ExcelWriter(output) as writer:
        frame.to_excel(writer)

    headers = {
        'Content-Disposition': 'attachment; filename="example.xlsx"'
    }
    return StreamingResponse(iter([output.getvalue()]), headers=headers)


class NameDatabase(BaseModel):
    name_db: str




def run_base(name_db):
    the_connection = Connection()
    c = the_connection.connect(name_db)
    b = Base(conn=c)
    v=b.get_base()
    today=date.today() 
    name_excel='Bases'+'-'+str(today)+'.xlsx'
    output = BytesIO()
    with pd.ExcelWriter(output,engine='openpyxl') as writer:   
        for dataframe in v:
            print('holi')
            area=dataframe['PRDN_AREA_CODE']
            area.drop_duplicates(inplace=True)
            area=area.values[0]
            dataframe.to_excel(writer,sheet_name=area)
    
    headers = {'Content-Disposition': 'attachment; filename={name}'.format(name=name_excel)}
    #return StreamingResponse(iter([output.getvalue()]), headers=headers)
"""