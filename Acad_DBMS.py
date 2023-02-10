import os
import time
import pymysql
import argparse

# System functions
clear = lambda : os.system('clear')


# Custom exception class for DB operations
class DBException(Exception):
  pass


# Utility functions

#Returns
def verify_rollNo(connection, rollNo):
  '''
  Function to check if a rollNo exists in student relation and return the student name
  params
   - connection: mysql connector
   - rollNo: (str) student roll number
  returns
   - rollNo_result: (tuple) the result of DB operation - (rollNo, name). None, if not found
  '''
  rollNo_result = None
  with connection.cursor() as cursor:
    sql_cmd = "SELECT rollNo, name FROM student WHERE rollNo = %s"
    cursor.execute(sql_cmd, (rollNo, ))
    rollNo_result = cursor.fetchone()
  
  return rollNo_result

def verify_deptId(connection, deptId):
  '''
  Function to check if a deptId exists in department relation and return the department name
  
  params
   - connection: mysql connector
   - deptId: (str) department Id
  returns
   - deptId_result: (tuple) the result of DB operation - (deptId, name). None, if not found
  '''
  deptId_result = None
  with connection.cursor() as cursor:
    sql_cmd = "SELECT deptId, name FROM department WHERE deptId = %s"
    cursor.execute(sql_cmd, (deptId, ))
    deptId_result = cursor.fetchone()
  
  return deptId_result

def verify_courseId_deptId(connection, courseId, deptId):
  '''
  Function to check if courseId in department deptId exists in course relation and return the course name
  
  params
   - connection: mysql connector
   - courseId: (str) course Id
   - deptId: (str) department Id
  returns
   - courseId_result: (tuple) the result of DB operation - (courseId, cname). None, if not found
  '''
  courseId_result = None
  with connection.cursor() as cursor:
    sql_cmd = "SELECT courseId, cname FROM course WHERE courseId = %s AND deptNo = %s"
    cursor.execute(sql_cmd, (courseId, deptId))
    courseId_result = cursor.fetchone()
  
  return courseId_result

def verify_courseId_offer(connection, courseId, year, sem):
  '''
  Function to check if the course is offered by a teacher in the given year and sem
  
  params
   - connection: mysql connector
   - courseId: (str) course Id
   - year : (int) course offered year 
   - sem : (str) semester of offering ('Even' or 'Odd')
  returns
   - course_offered: (bool) whether the course is offered in the given year and sem
  '''
  course_offered = True
  with connection.cursor() as cursor:
    offer_cmd = "SELECT * FROM teaching WHERE courseId = %s and sem = %s and year = %s"
    cursor.execute(offer_cmd, (courseId, sem, year))
    if cursor.fetchone() == None:
      course_offered = False
  
  return course_offered

def verify_profId(connection, profId):
  '''
  Function to check if a profId exists in professor relation and return the professor name
  
  params
   - connection: mysql connector
   - profId: (str) professor Id
  returns
   - profId_result: (tuple) the result of DB operation - (empId, name). None, if not found
  '''
  profId_result = None
  with connection.cursor() as cursor:
    sql_cmd = "SELECT empId, name FROM professor WHERE empId = %s"
    cursor.execute(sql_cmd, (profId, ))
    profId_result = cursor.fetchone()
  
  return profId_result

def validate_course_prof(connection, courseId, profId, year, sem):
  '''
  Function to check if professor profId teaches course courseId in the given year and sem, and return the classroom
  
  params
   - connection: mysql connector
   - deptId: (str) department Id
   - year : (int) course offered year 
   - sem : (str) semester of offering ('Even' or 'Odd')
  returns
   - result: (tuple) the result of DB operation - (empId, courseId, classroom). None, if not found
  '''
  result = None
  with connection.cursor() as cursor:
    sql_cmd = "SELECT empId, courseId, classroom FROM teaching WHERE empId = %s and courseId = %s and sem = %s and year = %s"
    cursor.execute(sql_cmd, (profId, courseId, sem, year))
    result = cursor.fetchone()
  
  return result

def update_teaching(connection, courseId, profId, year, sem, classroom):
  '''
  Function to update classroom of an already offered course in teaching table
  
  params:
   - connection - mysql connector
   - courseId - (str) courseId of offered course
   - profId - (str) profId of professor teaching the course
   - year : (int) course offered year 
   - sem : (str) semester of offering ('Even' or 'Odd')
   - classroom : (str) new value of classroom
  returns
   - none
  '''
  with connection.cursor() as cursor:
    sql_cmd = "UPDATE teaching SET classroom = %s WHERE empId = %s and courseId = %s and sem = %s and year = %s"
    cursor.execute(sql_cmd, (classroom, profId, courseId, sem, year))
  
  connection.commit()
  
def add_teaching(connection, courseId, profId, year, sem, classroom):
  '''
  Function to add new course offering in teaching table
  
  params:
   - connection - mysql connector
   - courseId - (str) courseId of offered course
   - profId - (str) profId of professor teaching the course
   - year : (int) course offered year 
   - sem : (str) semester of offering ('Even' or 'Odd')
   - classroom : (str) new value of classroom
  
  returns
   - none
  '''
  with connection.cursor() as cursor:
    sql_cmd = "INSERT INTO teaching(empId, courseId, sem, year, classroom) values (%s, %s, %s, %s, %s)"
    cursor.execute(sql_cmd, (profId, courseId, sem, year, classroom))
  
  connection.commit()

def add_enrollment(connection, courseId, rollNo, year, sem):
  '''
  Function to add new student enrolled in enrollment table
  
  params:
   - connection - mysql connector
   - courseId - (str) course Id of enrolled course
   - rollNo - (str) roll Number of student enrolled in the course
   - year : (int) year of enrollment
   - sem : (str) 'Even' or 'Odd' , semester of enrollment
  
  returns
   - none
  '''
  with connection.cursor() as cursor:
    sql_cmd = "INSERT INTO enrollment(rollNo, courseId, sem, year) values (%s, %s, %s, %s)"
    cursor.execute(sql_cmd, (rollNo, courseId, sem, year))
  
  connection.commit()

# Handlers for each action
def process_action1(connection, deptId):
  '''
  Function to add new offerings of courses by given department in year 2006 and even semester 
  param
  - connection : mysql connector
  - deptId : (str) given department Id
  returns
   - none
  '''
  try:
    courseId = str(input('> Enter courseId: '))
    
    # Verify that the given department offers course courseId
    result = verify_courseId_deptId(connection, courseId, deptId)
    if not result:
      raise DBException('Entered courseId is not offered by the current department')
    else:
      print(f'-- courseId {courseId} verified: {result[1]}\n')

    profId = str(input('> Enter profId: '))
    # Verify that the entered professor exists
    result =  verify_profId(connection, profId)
    if not result:
      raise DBException('Entered profId not found in the professor relation')
    else:
      print(f'-- profId {profId} verified: {result[1]}\n')

    classroom = str(input('> Enter classroom: '))
    
    # Check if the course and professor pair exists in the teaching relation already
    result = validate_course_prof(connection, courseId, profId, 2006, 'Even')
    if result:
      # If it exists, ask if overwriting the classroom is required
      print(f'\nNOTE: Given values already exists in the teaching relation!! Professor {profId} teaches course {courseId} in classroom {result[2]}\n')
      act = input('> Are you sure you wish to (over)write the classroom? (y/n) ')
      if act.lower() in ['y', 'yes']:
        update_teaching(connection, courseId, profId, 2006, 'Even', classroom)
        print('\n\nSuccessfully updated classroom!!')
    else :
      # If it doesn't exist, ask if it needs to inserted
      print('\nNOTE: Given course and professor pair does not exist in the teaching relation')
      act = input('> Are you sure you wish to add this course offering? (y/n) ')
      if act.lower() in ['y', 'yes']:
        add_teaching(connection, courseId, profId, 2006, 'Even', classroom)
        print(f'\n\nSuccessfully added course {courseId} by professor {profId} in classroom {classroom}!!')

  except Exception as e:
    print('\n-- Faced following error while processing action 1!! --')
    print()
    print(e)
    print()
    print('-------------------------------------------------------')


def process_action2(connection, deptId):
  '''
  Function to enrol students to courses offered by given department in year 2006 and even semester 
  param
  - connection : mysql connector
  - deptId : (str) given department Id
  returns
   - none
  '''
  try:
    rollNo = str(input('> Enter Roll Number: '))
    # Check whether rollNo is valid
    result = verify_rollNo(connection, rollNo)
    if not result:
      raise DBException('Entered rollNo is not found in the student relation')
    else:
      print(f'-- rollNo {rollNo} verified: {result[1]}\n')

    courseId = str(input('> Enter course ID: '))
    # Check whether courseId is valid and offered by deptId
    result = verify_courseId_deptId(connection, courseId, deptId)
    courseName = None
    if not result:
      raise DBException('Entered courseId is not offered by the current department')
    else:
      courseName = result[1]
  
    # Check whether courseId is offered in Even semester, 2006
    if not verify_courseId_offer(connection, courseId, 2006, 'Even'):
      raise DBException('Entered courseId is not offered in 2006, even semester')
    else:
      print(f'-- verified course {courseId}: {courseName} is offered in Even semester of 2006\n')

    # Query to check whether student rollNo have completed the preRequisited of course courseId
    notPassPreReq = "SELECT p.preReqCourse, c.cname FROM prerequisite as p, course as c WHERE p.courseId = %s \
                      AND c.courseId = p.preReqCourse AND p.preReqCourse NOT IN (SELECT courseId FROM enrollment\
                        WHERE rollNo = %s AND grade != 'U' AND grade IS NOT NULL \
                          AND year < 2007)"

    with connection.cursor() as cursor:
      cursor.execute(notPassPreReq, (courseId, rollNo))
      preReqCourses = cursor.fetchall()
      if len(preReqCourses) == 0:
        # Query to check whether student rollNo have already enrolled for course in current semester or completed the course before
        enrollCheck = "SELECT rollNo, year, sem, grade FROM enrollment \
                        WHERE rollNo = %s AND courseId = %s \
                        AND ((sem = 'Even' AND year = 2006) OR (grade != 'U' AND grade IS NOT NULL)) \
                        AND year < 2007"

        cursor.execute(enrollCheck, (rollNo, courseId))
        result = cursor.fetchone()
        if result == None:
          print('\nNOTE: Given student has passed all pre-requisites and can be enrolled to the course.')
          act = input('> Are you sure you wish to enrol the student? (y/n) ')
          if act.lower() in ['y', 'yes']:
            add_enrollment(connection, courseId, rollNo, 2006, 'Even')
            print('\n\nSuccessfully enrolled student {} to course {}!!'.format(rollNo, courseId))
        else:
          grade_str = grade_str = f' and passed with {result[3]} grade' if result[1] != 2006 or result[2] != 'Even' else ''
          raise DBException('Student {} has already enrolled for the course {} in {} semester, {}'.format(rollNo, courseId, result[2], result[1]) + grade_str)
      else:
        failedPreReq = ''
        for i in range(0, len(preReqCourses)):
          failedPreReq = failedPreReq + '\n' + preReqCourses[i][1] + ' (' + preReqCourses[i][0] + ')'

        raise DBException(("Student {} has not passed the following prequisites of course {}: {}".format(rollNo, courseId, failedPreReq)))

  except Exception as e:
    print('\n-- Faced following error while processing action 2!! --')
    print()
    print(e)
    print()
    print('-------------------------------------------------------')


# Main
if __name__ == '__main__':
  parser = argparse.ArgumentParser(description='(DBMS Assgn 4a) Application to add course and students to academic_insti database')
  parser.add_argument('-host', dest='host', type=str, default='localhost', help='host address of database (eg: localhost, remotemysql.com)')
  parser.add_argument('-port', dest='port', type=int, default=3306, help='port number used to connect to the DB host (MySQL default port: 3306)')
  parser.add_argument('-user', dest='user', type=str, default='root', help='user name to use to connect to DB')
  parser.add_argument('-pwd', dest='password', type=str, nargs='?', const='', default='', help='password for the user (dont pass any value or dont use the option if empty)')
  parser.add_argument('-db', dest='dbname', type=str, default='academic_inst', help='name of database to connect to')

  args = parser.parse_args()

  conn = pymysql.connect(
    host=args.host, 
    port=args.port,
    user=args.user, 
    passwd=args.password, 
    db=args.dbname
  )

  # get department id from user 
  clear()
  print()
  print('#############################################')
  print()
  deptId = str(input('> Enter department Id: '))
  print()
  print('#############################################')
  print()

  clear()
  print('Loading', end=' ', flush=True)
  for _ in range(3):
    time.sleep(0.5)
    print('.', end=' ', flush=True)
  print()
  
  result = verify_deptId(conn, deptId)
  if not result:
    print('Entered department Id not found in department relation. Quitting!!')
    time.sleep(2)
  else :
    print(f'Department verified! Entering {deptId}: {result[1]} department ...')
    time.sleep(2)
    
    while(1):
      clear()
      print()
      print('#############################################')
      print('############### WELCOME ADMIN ###############')
      print('#############################################')
      print(f'Department {deptId}: {result[1]}\n')
      print('> Choose the action you want to take:')
      print('  1. Add/Update course')
      print('  2. Enroll student')
      print('  3. Exit')

      key = input('\n> ')
      
      if key == '1':
        act = True
        while act:
          clear()
          print('############## ACTION 1: Add/Update course #############')
          print(f'Department {deptId}: {result[1]}\n')
          process_action1(conn, deptId)
          act_key = input('\n\n> Do you wish to repeat this action? (y/n) ')
          act = act_key.lower() == 'y' or act_key.lower() == 'yes'

      elif key == '2':
        act = True
        while act:
          clear()
          print('############## ACTION 2: Enroll student #############')
          print(f'Department {deptId}: {result[1]}\n')
          process_action2(conn, deptId)
          act_key = input('\n\n> Do you wish to repeat this action? (y/n) ')
          act = act_key.lower() == 'y' or act_key.lower() == 'yes'

      else:
        print('\n################## THANK YOU #################\n')
        break

  conn.close()