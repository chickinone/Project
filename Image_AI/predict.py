import torch
import torchvision.transforms as transforms
from torchvision import models
from PIL import Image
import torch.nn as nn

with open("classes.txt", "r") as f:
    class_names = [line.strip() for line in f]

device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
model = models.resnet18(pretrained=False)
model.fc = nn.Linear(model.fc.in_features, len(class_names))
model.load_state_dict(torch.load("animal_classifier.pth", map_location=device))
model.to(device)
model.eval()

# Predict function
def predict_image(image_path):
    image = Image.open(image_path).convert("RGB")
    transform = transforms.Compose([
        transforms.Resize((224, 224)),
        transforms.ToTensor(),
        transforms.Normalize([0.485, 0.456, 0.406],
                             [0.229, 0.224, 0.225])
    ])
    img_tensor = transform(image).unsqueeze(0).to(device)
    with torch.no_grad():
        output = model(img_tensor)
        probs = torch.nn.functional.softmax(output[0], dim=0)
        pred = torch.argmax(probs).item()
        print(f"[PREDICTION] {class_names[pred]} ({probs[pred]*100:.2f}%)")

predict_image("D:/Project/Image_AI/animals/butterfly/e030b20a20e90021d85a5854ee454296eb70e3c818b413449df6c87ca3ed_640.jpg")
