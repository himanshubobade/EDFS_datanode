const tree = document.getElementById('tree');

async function getFiles() {
  const response = await fetch('http://127.0.0.1:8080/files');
  console.log(response);
  const data = await response.json();

  return data.children;
}

function createTree(parent, children) {
  const ul = document.createElement('ul');
  children.forEach(child => {
    const li = document.createElement('li');
    const span = document.createElement('span');
    span.textContent = child.name;
    li.appendChild(span);
    ul.appendChild(li);
    if (child.children.length > 0) {
      li.classList.add('parent_li');
      createTree(li, child.children);
    }
  });
  parent.appendChild(ul);
}

getFiles().then(children => {
    createTree(tree, children);
});

// async function getFile() {
//   const response = await fetch("http://127.0.0.1:8080/file/user/edfs.py");
//   s = await response.text()
//   const anchor = document.createElement("pre");
//   // anchor.style.wordWrap = "break-word";
//   anchor.style.whiteSpace = "pre-wrap";
//   anchor.style.font = "monospace";
//   anchor.innerHTML = s;
//   document.body.appendChild(anchor);
// }

// // getFile().then();


const uploadForm = document.getElementById('uploadForm')
uploadForm.addEventListener('submit', function(e) {
  e.preventDefault();
  let file = e.target.uploadFile.files[0];
  let remotePath = e.target.remotePath.value;
  console.log(remotePath)
  let formData = new FormData();
  formData.append('file', file);

  fetch(`http://127.0.0.1:8080/upload/${remotePath}`, {
    method: 'POST',
    body: formData
  }).then(
    response => response.json()
  ).then(
    data => {
      console.log(data);
    }
  );
})
