'''
BFS알고리즘 이용해서 그래프 검색
두 노드 사이의 거리를 알아내기
헐크->아이언맨 사이의 최소거리 구하기
#1. 각 행을 노드를 구조화된 데이터(노드)로 변경
-ex. 5983 1165 3836 4361 1282 ->(5983,(1165,3836,4361,1282),9999,WHITE)=(heroID,(connections,distance,color))
'''
#Boilerplate stuff:
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("DegreesOfSeparation")
sc = SparkContext(conf = conf)

# The characters we wish to find the degree of separation between:
startCharacterID = 5306 #SpiderMan
targetCharacterID = 14  #ADAM 3,031 (who?)

# Our accumulator, used to signal when we find the target character during
# our BFS traversal.
hitCounter = sc.accumulator(0) #초기값 0으로 설정

#그래프의 초기조건 설정하고 입력데이터를 노드 구조로 변환

def convertToBFS(line):
    fields = line.split()
    heroID = int(fields[0])
    connections = []
    for connection in fields[1:]:
        connections.append(int(connection))

    color = 'WHITE'#초기화 조건: 처리되지 않은 노드는 흰색
    distance = 9999 #초기화 조건:노드는 모두 무제한으로 멀리 있음

    if (heroID == startCharacterID):
        color = 'GRAY'#노드가 확장되야하는 대상->회색
        distance = 0
        
    return (heroID, (connections, distance, color))#키값 쌍으로 변환


def createStartingRdd():
    inputFile = sc.textFile("file:///sparkcourse/marvel-graph.txt")
    return inputFile.map(convertToBFS)

#각 노드에서 정보를 추출하고 회색노드를 찾아 폭파한 다음 검은색으로 칠해 연결해서 회색노들 생성
def bfsMap(node):
    characterID = node[0]
    data = node[1]
    connections = data[0]
    distance = data[1]
    color = data[2]

    results = []

    #If this node needs to be expanded...
    if (color == 'GRAY'):
        for connection in connections:
            newCharacterID = connection
            newDistance = distance + 1 
            newColor = 'GRAY'
            if (targetCharacterID == connection):
                hitCounter.add(1) #찾던 캐릭터와 마주치면 accumulator의 Counter 1 증가시킴

            newEntry = (newCharacterID, ([], newDistance, newColor))
            results.append(newEntry)

        #We've processed this node, so color it black
        color = 'BLACK'

    #Emit the input node so we don't lose it.
    results.append( (characterID, (connections, distance, color)) )
    return results

def bfsReduce(data1, data2):
    edges1 = data1[0]
    edges2 = data2[0]
    distance1 = data1[1]
    distance2 = data2[1]
    color1 = data1[2]
    color2 = data2[2]

    distance = 9999
    color = color1
    edges = []

    # See if one is the original node with its connections.
    # If so preserve them.
    if (len(edges1) > 0):
        edges.extend(edges1)
    if (len(edges2) > 0):
        edges.extend(edges2)

    # Preserve minimum distance
    if (distance1 < distance):
        distance = distance1

    if (distance2 < distance):
        distance = distance2

    # Preserve darkest color
    if (color1 == 'WHITE' and (color2 == 'GRAY' or color2 == 'BLACK')):
        color = color2

    if (color1 == 'GRAY' and color2 == 'BLACK'):
        color = color2

    if (color2 == 'WHITE' and (color1 == 'GRAY' or color1 == 'BLACK')):
        color = color1

    if (color2 == 'GRAY' and color1 == 'BLACK'):
        color = color1

    return (edges, distance, color)


#Main program here:
iterationRdd = createStartingRdd()

for iteration in range(0, 10):#최대 10번 반복
    print("Running BFS iteration# " + str(iteration+1))

    # Create new vertices as needed to darken or reduce distances in the
    # reduce stage. If we encounter the node we're looking for as a GRAY
    # node, increment our accumulator to signal that we're done.
    mapped = iterationRdd.flatMap(bfsMap) #flatmap 호출

    # Note that mapped.count() action here forces the RDD to be evaluated, and
    # that's the only reason our accumulator is actually updated.
    print("Processing " + str(mapped.count()) + " values.")

    if (hitCounter.value > 0):
        print("Hit the target character! From " + str(hitCounter.value) \
            + " different direction(s).")
        break

    # Reducer combines data for each character ID, preserving the darkest
    # color and shortest path.
    iterationRdd = mapped.reduceByKey(bfsReduce)
