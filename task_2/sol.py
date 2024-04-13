import numpy as np
import time

start = time.time()
a = np.random.randint(0, 10, size=(1000, 1000))
end = time.time()

creation_time = end - start
print("Time taken to create the array:", creation_time, "seconds")

buffer_array = a.tobytes()
recreated_array = np.frombuffer(buffer_array, dtype=a.dtype).reshape(a.shape)
print("Arrays are equal:", np.array_equal(a, recreated_array))
