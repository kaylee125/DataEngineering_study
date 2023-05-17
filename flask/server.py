from flask import Flask, request, redirect

app = Flask(__name__)

nextId=4
topics = [
    {'id': 1, 'title': 'html', 'body': 'html is ...'},
    {'id': 2, 'title': 'css', 'body': 'css is ...'},
    {'id': 3, 'title': 'javascript', 'body': 'javascript is ...'}
]

#전체 템플릿을 함수화하여 중복코드 제거
def template(contents,content,id=None):
    contextUI=''
    if id !=None:
        contextUI=f'''
            <li><a href="/update/{id}/">update</a></li>
            <li><form action ="/delete/{id}/" method="POST"><input type="submit" value="delete"></form></li>
        '''

    return f'''<!doctype html>
    <html>
        <body>
            <h1><a href="/">WEB</a></h1>
            <ol>
                {contents}
            </ol>
            {content}
             <ul>
                <li><a href="/create/">create</a></li>
                {contextUI}
            </ul>
        </body>
    </html>
    '''
#liTags부분 함수화 
def getContents():
    liTags = ''
    for topic in topics:
        liTags = liTags + f'<li><a href="/read/{topic["id"]}/">{topic["title"]}</a></li>'
    return liTags

#home화면
@app.route('/')
def index():
    return template(getContents(),'<h2>Welcome</h2>hello web')
#읽기:Read
@app.route('/read/<int:id>/')
def read(id):
    liTags=''
    for topic in topics:
        liTags=liTags + f'<li><a href="/read/{topic["id"]}/">{topic["title"]}</a></li>'
        title=''
        body=''
    for topic in topics:
        if id==topic['id']:
            title=topic['title']
            body=topic['body']
            break

    return template(getContents(), f'<h2>{title}</h2>{body}',id)

@app.route('/create/',methods=['GET','POST']) #해당 라우터가 허용하는 method 지정
def create():
    if request.method=='POST':
        global nextId
        #입력한 값 name태그로 알아내기
        title=request.form['title']
        body=request.form['body']
        newTopic={'id':nextId,'title':title,'body':body}
        topics.append(newTopic)
        url='/read/'+str(nextId)+'/' #생성한 글의 url로 이동
        nextId+=1 #다음을 위해 Nextid 1증가
        return redirect(url);''
    else:
        #create UI생성 
        content='''
            <form action="/create/" method="POST"> 
                <p><input type="text" name='title' placeholder='title'></p>
                <p><textarea name='body' placeholder='body'></textarea></p>
                <p><input type='submit' value='create'></p>
            </form>
        '''
        return template(getContents(),content)

@app.route('/update/<int:id>/',methods=['GET','POST']) #해당 라우터가 허용하는 method 지정
def update(id):
    if request.method=='POST':
        global nextId
        #입력한 값 name태그로 알아내기
        title=request.form['title']
        body=request.form['body']
        #기존 갑 수정
        for topic in topics:
            if id==topic['id']:
                topic['title']=title
                topic['body']=body
                break
        url='/read/'+str(id)+'/' #생성한 글의 url로 이동
        return redirect(url)
    else:
        #타이틀과 바디 값을 검색한 다음 vlaue값으로 전달
        title=''
        body=''
        for topic in topics:
            if id==topic['id']:
                title=topic['title']
                body=topic['body']
                break
        #create UI생성 
        content=f'''
            <form action="/update/{id}/" method="POST"> 
                <p><input type="text" name='title' placeholder='title' value={title}></p>
                <p><textarea name='body' placeholder='body'>{body}</textarea></p>
                <p><input type='submit' value='update'></p>
            </form>
        '''
        return template(getContents(),content)

@app.route('/delete/<int:id>/',methods=['POST'])
def delete(id):
    for topic in topics:
        if id==topic['id']:
            topics.remove(topic)
            break    
    return redirect('/')
app.run(debug=True)