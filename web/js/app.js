api_url = "localhost/api/";

const rating = document.getElementById("rating");
const cuzine = document.getElementById("cuzine");
const location = document.getElementById("location");
const submit = document.getElementById("submit");
const reset = document.getElementById("reset");

console.log(api_url)

submit.addEventListener("click", function () {
  
  //get value of dropdowm control
  var rating = rating.value;
  var cuzine = cuzine.value;
  var location = location.value;

   //check if value is empty
  if (location == "") {
    alert("Please select a location");
    return false;
  }
  if (cuzine == "") {
    alert("Please select a cuzine");
    return false;
  }  
  if (rating == "") {
    alert("Please select a rating");
    return false;
  }

  const data = {
    rating: rating.value,
    cuzine: cuzine.value,
    location: location.value,
  };
  console.log(data);
  
  fetch(api_url, {
    method: "POST",
    body: JSON.stringify(data),
    headers: {
      "Content-Type": "application/json",
    },
  })
    .then((response) => response.json())
    .then((data) => {
      console.log("Success:", data);
    })
    .catch((error) => {
      console.error("Error:", error);
    });
});

function resetform {
  //get value of dropdowm control
  var rating = rating.value;
  var cuzine = cuzine.value;
  var location = location.value;
  //check if value is empty
  if (rating == "") {
    alert("Please select a rating");
    return false;
  }
  if (cuzine == "") {
    alert("Please select a cuzine");
    return false;
  }  
  if (location == "") {
    alert("Please select a location");
    return false;
  }

}

