import modal
from modal import Image

image = (
    Image.debian_slim(python_version="3.9")
    .copy_local_file("./requirements.txt", ".")
    .copy_local_file("./utils.py", ".")
    .copy_local_dir("../llama", "../llama")
    .copy_local_dir("../src", "../src")
    .copy_local_dir("../img", "../img")
    .apt_install("git")
    .pip_install("torch==2.0.0", "torchvision==0.15.1", "-f", "https://download.pytorch.org/whl/cu117")
    .pip_install("git+https://github.com/usuyama/open_clip.git@01a53cc46662d8b28da0c9d73271bfb8f6f4b20d")
    .pip_install("h5py", "ipykernel")
    .pip_install_from_requirements("requirements.txt")

)
app = modal.App(
    "med-flamingo-demo"
)

with image.imports():
    from huggingface_hub import hf_hub_download
    import torch
    import os
    from open_flamingo import create_model_and_transforms
    from accelerate import Accelerator
    from einops import repeat
    from PIL import Image
    import sys
    sys.path.append('..')
    from src.utils import FlamingoProcessor
    from demo_utils import image_paths, clean_generation



@app.function(image=image, gpu="A100", timeout=6000)
def run_model():
    accelerator = Accelerator() #when using cpu: cpu=True

    device = accelerator.device
    
    print('Loading model..')

    # >>> add your local path to Llama-7B (v1) model here:
    llama_path = '../llama/llama-7b-hf'
    if not os.path.exists(llama_path):
        raise ValueError('Llama model not yet set up, please check README for instructions!')

    model, image_processor, tokenizer = create_model_and_transforms(
        clip_vision_encoder_path="ViT-L-14",
        clip_vision_encoder_pretrained="openai",
        lang_encoder_path=llama_path,
        tokenizer_path=llama_path,
        cross_attn_every_n_layers=4
    )

    print("Loading Med-Flamingo checkpoint")
    # load med-flamingo checkpoint:
    checkpoint_path = hf_hub_download("med-flamingo/med-flamingo", "model.pt")
    print(f'Downloaded Med-Flamingo checkpoint to {checkpoint_path}')
    model.load_state_dict(torch.load(checkpoint_path, map_location=device), strict=False)
    processor = FlamingoProcessor(tokenizer, image_processor)
    
    print("Evaluating model")
    # go into eval model and prepare:
    model = accelerator.prepare(model)
    is_main_process = accelerator.is_main_process
    model.eval()

    """
    Step 1: Load images
    """
    demo_images = [Image.open(path) for path in image_paths]

    """
    Step 2: Define multimodal few-shot prompt 
    """

    # example few-shot prompt:
    prompt = "You are a helpful medical assistant. You are being provided with images, a question about the image and an answer. Follow the examples and answer the last question. <image>Question: What is/are the structure near/in the middle of the brain? Answer: pons.<|endofchunk|><image>Question: Is there evidence of a right apical pneumothorax on this chest x-ray? Answer: yes.<|endofchunk|><image>Question: Is/Are there air in the patient's peritoneal cavity? Answer: no.<|endofchunk|><image>Question: Does the heart appear enlarged? Answer: yes.<|endofchunk|><image>Question: What side are the infarcts located? Answer: bilateral.<|endofchunk|><image>Question: Which image modality is this? Answer: mr flair.<|endofchunk|><image>Question: Where is the largest mass located in the cerebellum? Answer:"
    
    """
    Step 3: Preprocess data 
    """
    print('Preprocess data')
    pixels = processor.preprocess_images(demo_images)
    pixels = repeat(pixels, 'N c h w -> b N T c h w', b=1, T=1)
    tokenized_data = processor.encode_text(prompt)

    """
    Step 4: Generate response 
    """

    # actually run few-shot prompt through model:
    print('Generate from multimodal few-shot prompt')
    generated_text = model.generate(
    vision_x=pixels.to(device),
    lang_x=tokenized_data["input_ids"].to(device),
    attention_mask=tokenized_data["attention_mask"].to(device),
    max_new_tokens=10,
    )
    response = processor.tokenizer.decode(generated_text[0])
    response = clean_generation(response)

    print(f'{response=}')

