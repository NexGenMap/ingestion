
var asset = "projects/nexgenmap/MapBiomas2/PLANET/images";

var collection = ee.ImageCollection(asset)
    .filterMetadata('tile', 'equals', '1635541');


var imageNames = collection.reduceColumns(ee.Reducer.toList(), ['system:index'])
    .get('list')
    .getInfo()
    .map(
        function(imageId){
            return asset + '/' + imageId;
        }
    );

var Enhance = require('users/joaovsiqueira1/packages:Enhance.js');

/**
 * Enhances an image applying a 2% streching
 * 
 * @param {ee.Image} image
 * @param {ee.Geometry} geometry
 * @returns {ee.Image} imageEnhanced
 */
var enhance = function (image) {

    var imageEnhanced = Enhance.linear2perc(
        image.select(['R', 'G', 'B'], ['r', 'g', 'b'])
    );

    return imageEnhanced;
};

imageNames.forEach(
    function (imageName) {
        print(imageName);
        var image = ee.Image(imageName);

        Map.addLayer(enhance(image), {}, imageName.split('/')[5]);
        Map.centerObject(image);
    }
);

