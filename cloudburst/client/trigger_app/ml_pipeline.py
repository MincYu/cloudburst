from cloudburst.client.ephe_common import *
from anna.lattices import LWWPairLattice

model_key = 'mobilenet-model'
label_map_key = 'placeholder'
# with open('mobilenet_v2_1.4_224_frozen.pb', 'rb') as f:
#     bts = f.read()
#     lattice = LWWPairLattice(0, bts)
#     cloudburst_client.kvs_client.put(model_key, lattice)

def dag_preprocess(cloudburst, inp):
    start_t = int(time.time() * 1000000)
    import PIL.Image as Image
    # res = filters.gaussian(inp).reshape(1, 224, 224, 3)
    img = Image.open("/dev/shm/grace_hopper.jpg").resize((224, 224))
    img = np.array(img) / 255.0
    img = img.reshape((1,224,224,3))
    end_t = int(time.time() * 1000000)
    print(f'Preprocess function start: {start_t}, end: {end_t}')
    return img

class Mobilenet:
    def __init__(self, cloudburst, model_key, label_map_key):
        import tensorflow as tf
        import json

        tf.enable_eager_execution()

        self.model = cloudburst.get(model_key, deserialize=False)
        # self.label_map = json.loads(cloudburst.get(label_map_key,
        #                                         deserialize=False))

        # self.gd = tf.GraphDef.FromString(self.model)
        # self.inp, self.predictions = tf.import_graph_def(self.gd,
        #                                                     return_elements=['input:0', 'MobilenetV2/Predictions/Reshape_1:0'])
        graph = tf.Graph()
        graph_def = tf.GraphDef.FromString(self.model)
        with graph.as_default():
            tf.import_graph_def(graph_def)
        self.sess = tf.Session(graph=graph)

        self.input_operation = graph.get_operation_by_name("import/input")
        self.output_operation = graph.get_operation_by_name("import/MobilenetV2/Predictions/Reshape_1")

    def run(self, cloudburst, img):
        start_t = int(time.time() * 1000000)
        # load libs
        # import tensorflow as tf
        # from PIL import Image
        # from io import BytesIO
        # import base64
        # import numpy as np
        # import json

        # tf.enable_eager_execution()

        # load image and model
        # img = np.array(Image.open(BytesIO(base64.b64decode(img))).resize((224, 224))).astype(np.float) / 128 - 1
        # with tf.Session(graph=self.inp.graph):
        #     x = self.predictions.eval(feed_dict={self.inp: img})
        
        x = self.sess.run(self.output_operation.outputs[0], {
            self.input_operation.outputs[0]: img
        })
        end_t = int(time.time() * 1000000)
        print(f'Inference function start: {start_t}, end: {end_t}')
        return x

def dag_average(cloudburst, inp1, inp2, inp3):
    start_t = int(time.time() * 1000000)
    import numpy as np
    inp = [inp1, inp2, inp3]
    res = np.mean(inp, axis=0)
    end_t = int(time.time() * 1000000)
    print(f'Average function start: {start_t}, end: {end_t}')
    return res

# def dag_preprocess(cloudburst, key):
#     start = time.time()
#     cloudburst.put('start_', start, durable=True)
#     from skimage import filters
#     inp = cloudburst.get(key, durable=True)
#     return filters.gaussian(inp).reshape(1, 3, 224, 224)

# def dag_sqnet(cloudburst, inp):
#     import torch
#     import torchvision
#     model = torchvision.models.squeezenet1_1()
#     return model(torch.tensor(inp.astype(np.float32))).detach().numpy()

# def dag_average(cloudburst, inp1, inp2, inp3):
#     import numpy as np
#     inp = [inp1, inp2, inp3]
#     res = np.mean(inp, axis=0)
#     end = time.time()
#     cloudburst.put('end_', end, durable=True)
#     return res

dag_name = 'dag_pipeline'

# suc, err = cloudburst_client.delete_dag(dag_name)
# exit(0)

key_n = 'image'
arr = np.random.randn(1, 224, 224, 3)
# cloudburst_client.put_object(key_n, arr)

cloud_prep = cloudburst_client.register(dag_preprocess, 'preprocess')
cloud_mnet1 = cloudburst_client.register((Mobilenet, (model_key, label_map_key)), 'net1')
cloud_mnet2 = cloudburst_client.register((Mobilenet, (model_key, label_map_key)), 'net2')
cloud_mnet3 = cloudburst_client.register((Mobilenet, (model_key, label_map_key)), 'net3')
# cloud_sqnet1 = cloudburst_client.register(dag_sqnet, 'sqnet1')
# cloud_sqnet2 = cloudburst_client.register(dag_sqnet, 'sqnet2')
# cloud_sqnet3 = cloudburst_client.register(dag_sqnet, 'sqnet3')
cloud_average = cloudburst_client.register(dag_average, 'average')


functions = ['preprocess', 'net1', 'net2', 'net3', 'average']
connections = [('preprocess', 'net1'), ('preprocess', 'net2'), ('preprocess', 'net3'),
            ('net1', 'average'), ('net2', 'average'), ('net3', 'average')]
success, error = cloudburst_client.register_dag(dag_name, functions, connections)

arg_map = {'preprocess': [0]}

elasped_list = []
cloudburst_client.call_dag(dag_name, arg_map, True)
for _ in range(100):
    start = time.time()
    cloudburst_client.call_dag(dag_name, arg_map, True)
    end = time.time()
    # start = cloudburst_client.get('start_')
    # end = cloudburst_client.get('end_')
    elasped_list.append(end - start)
    time.sleep(0.2)

print('dag results: elasped {}'.format(elasped_list))
suc, err = cloudburst_client.delete_dag(dag_name)

"""
Create bucket and add triggers for coordination.

coord_addr = ''
client = CoordClient(coord_addr, None)

client.create_bucket('pre', SESSION)
client.create_bucket('result', SESSION)

client.add_trigger('pre', 't1', UPON_WRITE, {'function': 'm1'})
client.add_trigger('pre', 't2', UPON_WRITE, {'function': 'm2'})

client.add_trigger('result', 't3', BY_SET, {'function': 'avg', 'key_set': ['re_1', 're_2']})

"""