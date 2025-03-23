# Importujemy wymagane klasy
from multiprocessing.managers import BaseManager
import queue

# Tworzymy klasę QueueManager dziedziczącą po BaseManager
class QueueManager(BaseManager):
    pass

def main():
    # Tworzymy kolejki
    qData = queue.Queue()
    qResults = queue.Queue()

    # Rejestrujemy kolejki w managerze
    QueueManager.register('get_queue_data', callable=lambda: qData)
    QueueManager.register('get_queue_results', callable=lambda: qResults)

    # Uruchamiamy serwer managera
    m = QueueManager(address=('localhost', 50000), authkey=b'abracadabra')
    s = m.get_server()  # Tworzymy serwer
    print("Manager server is running...")  # Informacja o uruchomieniu serwera
    s.serve_forever()  # Uruchamiamy serwer

if __name__ == '__main__':
    main()  # Uruchamiamy funkcję main()
