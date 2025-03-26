# Importujemy wymagane klasy i funkcje
from multiprocessing.managers import BaseManager
import sys
import time

# Tworzymy klasę QueueManager dziedziczącą po BaseManager
class QueueManager(BaseManager):
    pass

# Rejestrujemy kolejki, które będą wykorzystywane do wymiany danych z serwerem
QueueManager.register('get_queue_data')
QueueManager.register('get_queue_results')

def read(fname):
    try:
        with open(fname, "r", encoding="utf-8") as f:
            data = f.readlines()
            for i in range(len(data)):
                data[i] = data[i].rstrip()
        return data
    except FileNotFoundError:
        print(f"Błąd: Plik '{fname}' nie został znaleziony.")
        sys.exit(1)
    except ValueError as ve:
        print(f"Błąd: Nieprawidłowy format danych w pliku '{fname}'. {ve}")
        sys.exit(1)
    except Exception as e:
        print(f"Błąd przy odczycie pliku '{fname}': {e}")
        sys.exit(1)

def split_indices(total, parts):
    """
    Funkcja dzieląca data na części (indeksy).
    """
    base = total // parts  # Podstawowy rozmiar
    remainder = total % parts  # Reszta do podziału
    indices = []  # Lista indeksów
    start = 0  # Początkowy indeks

    # Dzielimy data na części
    for i in range(parts):
        extra = 1 if i < remainder else 0  # Dodatkowy element w przypadku reszty
        end = start + base + extra  # Końcowy indeks
        indices.append((start, end))  # Dodajemy zakres
        start = end  # Przesuwamy początkowy indeks na koniec poprzedniego zakresu
    return indices

def main():
    start_time = time.time()

    # Sprawdzamy argumenty wejściowe
    if len(sys.argv) < 3:
        print("Użycie: python client.py <liczba_zadań> <plik z danymi>")
        sys.exit(1)

    try:
        num_tasks = int(sys.argv[1])
        if num_tasks < 1:
            print("Liczba tasków nie może być < 1")
            sys.exit(1)
        fname = sys.argv[2]
    except ValueError:
        print("Błąd: <liczba_zadań> musi być liczbą całkowitą.")
        sys.exit(1)
    except Exception as e:
        print(f"Błąd podczas przetwarzania argumentów: {e}")
        sys.exit(1)

    # Wczytujemy data z plików
    data = read(fname)

    m = len(data)  # Liczba wierszy tablicy z danymi

    # Łączymy się z managerem
    try:
        mng = QueueManager(address=('192.168.0.38', 50000), authkey=b'abracadabra')
        mng.connect()
    except Exception as e:
        print(f"Błąd przy łączeniu z managerem: {e}")
        sys.exit(1)

    try:
        dataQueue = mng.get_queue_data()  # Pobieramy kolejkę danych
        resultsQueue = mng.get_queue_results()  # Pobieramy kolejkę wyników
    except Exception as e:
        print(f"Błąd przy pobieraniu kolejek: {e}")
        sys.exit(1)

    # Dzielimy zadania na części
    partitions = split_indices(m, num_tasks)

    for start, end in partitions:
        dane_block = data[start:end]  
        dataQueue.put((dane_block))  # Wysyłamy fragment do kolejki

    for _ in range(num_tasks):
        dataQueue.put(None) # death pills

    final_result = 0  #zmienna na wynik koncowy

    print("Client: Czekam na wyniki...")
    received_tasks = 0
    while received_tasks < num_tasks:
        print("Currently received tasks: ", received_tasks)
        block_result = resultsQueue.get()  # Pobieramy wynik z kolejki
        final_result += block_result
        received_tasks += 1
    print("Received all tasks")
    final_result /= m

    # Wyświetlamy wynik końcowy
    print("Client: Wynik końcowy (Średnia ilość cyfr w wierszu):")
    print(final_result)

    end_time = time.time()  # Zapisujemy czas końcowy
    elapsed_time = end_time - start_time  # Obliczamy czas wykonania
    print(f"Client: Czas wykonania: {elapsed_time:.4f} sekund")

if __name__ == '__main__':
    main()  # Uruchamiamy funkcję main()
