import torch
import torch.nn as nn
import torch.optim as optim
from torchvision import datasets, models, transforms
from torch.utils.data import DataLoader
import matplotlib.pyplot as plt
from PIL import Image
import os


DATA_DIR = r"D:/Project/Image_AI/animals"
MODEL_PATH = "animal_classifier.pth"
CLASSES_TXT = "classes.txt"
EPOCHS = 10
BATCH_SIZE = 32
LR = 1e-4


device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
print(f"[INFO] Using device: {device}")


transform_train = transforms.Compose([
    transforms.Resize((224, 224)),
    transforms.RandomHorizontalFlip(),
    transforms.RandomRotation(10),
    transforms.ColorJitter(brightness=0.2, contrast=0.2),
    transforms.ToTensor(),
    transforms.Normalize([0.485, 0.456, 0.406],
                         [0.229, 0.224, 0.225])
])

dataset = datasets.ImageFolder(root=DATA_DIR, transform=transform_train)
dataloader = DataLoader(dataset, batch_size=BATCH_SIZE, shuffle=True)
class_names = dataset.classes
print(f"[INFO] Found {len(dataset)} images across {len(class_names)} classes")

model = models.resnet18(weights="IMAGENET1K_V1") 
model.fc = nn.Linear(model.fc.in_features, len(class_names))
model = model.to(device)


criterion = nn.CrossEntropyLoss()
optimizer = optim.Adam(model.parameters(), lr=LR)


train_losses = []

for epoch in range(EPOCHS):
    model.train()
    running_loss = 0.0
    for imgs, labels in dataloader:
        imgs, labels = imgs.to(device), labels.to(device)
        optimizer.zero_grad()
        outputs = model(imgs)
        loss = criterion(outputs, labels)
        loss.backward()
        optimizer.step()
        running_loss += loss.item()

    avg_loss = running_loss / len(dataloader)
    train_losses.append(avg_loss)
    print(f"[EPOCH {epoch+1}/{EPOCHS}] - Loss: {avg_loss:.4f}")

torch.save(model.state_dict(), MODEL_PATH)
with open(CLASSES_TXT, "w") as f:
    for c in class_names:
        f.write(c + "\n")

print(f"[INFO] Model saved to {MODEL_PATH}")
print(f"[INFO] Classes saved to {CLASSES_TXT}")

def predict_image(image_path):
    model.eval()
    img = Image.open(image_path).convert("RGB")
    transform = transforms.Compose([
        transforms.Resize((224, 224)),
        transforms.ToTensor(),
        transforms.Normalize([0.485, 0.456, 0.406],
                             [0.229, 0.224, 0.225])
    ])
    img_tensor = transform(img).unsqueeze(0).to(device)  
    with torch.no_grad():
        output = model(img_tensor)  
        prob = torch.nn.functional.softmax(output[0], dim=0)
        pred = torch.argmax(prob).item()
        print(f"[PREDICTION] {class_names[pred]} ({prob[pred]*100:.2f}%)")

predict_image("D:/Project/Image_AI/animals/butterfly/e030b20a20e90021d85a5854ee454296eb70e3c818b413449df6c87ca3ed_640.jpg")
