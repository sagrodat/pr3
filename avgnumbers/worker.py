# Importujemy wymagane klasy i funkcje
from multiprocessing.managers import BaseManager
import sys
import time
from multiprocessing import Process, cpu_count

# Tworzymy klasę QueueManager dziedziczącą po BaseManager
class QueueManager(BaseManager):
    pass

# Rejestrujemy kolejki, które będą wykorzystywane do wymiany danych z serwerem
QueueManager.register('get_queue_data')
QueueManager.register('get_queue_results')

def worker_process(dataQueue, resultsQueue):
    """
    Funkcja robocza, która będzie wykonywać zadania pobierane z kolejki dataQueue i zapisujące wyniki do resultsQueue.
    """
    while True:
        task = dataQueue.get()  # Pobieramy zadanie z kolejki
        if task == None :
            break # death pill koniec

        # Rozpakowujemy dane zadania
        partial_result = 0
        for line in task :
            for sign in line :
                if sign.isnumeric() :
                    partial_result += 1

        resultsQueue.put((partial_result))  # Wysyłamy wynik do kolejki wyników

def main():

    if len(sys.argv) < 2:
        print("Użycie: python client.py <liczba_zadań>")
        sys.exit(1)

    try:
        num_tasks = int(sys.argv[1])
        if num_tasks < 1:
            print("Liczba tasków nie może być < 1")
            sys.exit(1)
    except ValueError:
        print("Błąd: <liczba_zadań> musi być liczbą całkowitą.")
        sys.exit(1)
    except Exception as e:
        print(f"Błąd podczas przetwarzania argumentów: {e}")
        sys.exit(1)


    try:
        # Łączymy się z serwerem managera
        mng = QueueManager(address=('localhost', 50000), authkey=b'abracadabra')
        mng.connect()
        dataQueue = mng.get_queue_data()  # Pobieramy kolejkę danych
        resultsQueue = mng.get_queue_results()  # Pobieramy kolejkę wyników
    except Exception as e:
        print(f"Błąd przy łączeniu z managerem lub pobieraniu kolejek: {e}")
        sys.exit(1)

    # Tworzymy procesy robocze (workery) do obróbki zadań
    #num_workers = cpu_count()  # Liczba procesów roboczych
    print(f"Worker: Uruchamiam {num_tasks} podprocesów.")


    workers = []
    for _ in range(num_tasks):
        p = Process(target=worker_process, args=(dataQueue, resultsQueue))  # Tworzymy proces
        p.start()  # Uruchamiamy proces
        workers.append(p)  # Dodajemy proces do listy

    # Czekamy aż wszystkie procesy zakończą swoją pracę
    for p in workers:
        p.join()

    print("Worker: Wszystkie procesy robocze zakończyły pracę.")

if __name__ == '__main__':
    main()  # Uruchamiamy funkcję main()
