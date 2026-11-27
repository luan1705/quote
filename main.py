from streaming.list import HOSE, HNX, UPCOM, DERIVATIVES, BOND
from streaming.quote import stream_symbol_quote


# SYMBOLS = HOSE + HNX + UPCOM + DERIVATIVES + BOND
SYMBOLS = ["*"]

STREAMS = {
    "symbol-quote": (stream_symbol_quote, SYMBOLS),
}


if __name__ == "__main__":
    from multiprocessing import Process
    from multiprocessing.connection import wait

    processes = []
    for stream_function, symbols in STREAMS.values():
        process = Process(
            target=stream_function,
            args=(symbols,),
            daemon=True,
        )
        processes.append(process)

    for process in processes:
        process.start()

    wait([process.sentinel for process in processes])

    # Có stream chết thì thoát lỗi để Docker restart container.
    raise SystemExit(1)
