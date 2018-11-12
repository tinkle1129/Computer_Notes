import subprocess

def monitor():
    subprocess.call("clear")  # linux/mac
    #  subprocess.call("cls", shell=True)  # windows
    print('#----------------------------------------------------#')
    print('#              Processsing HomeWork                  #')
    print('#----------------------------------------------------#')


def main_menu():
    print('#    There are two storage in this system            #')
    print('#    0: Processsing Operator      1: Calculate       #')
    print('#----------------------------------------------------#')


def processing_operator_menu():
    print('#              1. Create new process                 #')
    print('#              2. View running process               #')
    print('#              3. View  ready  process               #')
    print('#              4. View blocking process              #')
    print('#              5. block specified process            #')
    print('#              6. View   all   process               #')
    print('#              7. Wakeup  specified  process         #')
    print('#              8. Kill  specified  process           #')
    print('#              other:     Exit                       #')
    print('#----------------------------------------------------#')


def calculating_operator_menu():
    print('#            1. First Come First Served              #')
    print('#            2.   Priority Scheduling                #')
    print('#            3.    Short Job First                   #')
    print('#            4. Highest Response Ratio Next          #')
    print('#              other:     Exit                       #')
    print('#----------------------------------------------------#')


