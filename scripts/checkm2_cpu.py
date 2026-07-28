#!/usr/bin/env python3

import os

os.environ["KERAS_BACKEND"] = "tensorflow"

import tensorflow as tf

# 在 CheckM2 导入 Keras 模型之前禁用全部 GPU
tf.config.set_visible_devices([], "GPU")

from checkm2.main import main

if __name__ == "__main__":
    main()